import re
from datetime import datetime, timezone
from copy import copy
import functools
import itertools
from typing import List

from bson import ObjectId
from pydash import py_

from maggtomic import (
    prefix_expand,
    as_of,
    db as mdb,
    _oids_for,
    E,
    A,
    V,
    OID_URIREF,
    OID_VAEM_ID,
    PREFIXES,
    URI_BEGINNING_PATTERN,
)
from maggtomic.util import encode_id


def compile_graph_pattern(graph_pattern, use_prefixes=None, coll_hof=None):
    """prefix_expand and get oids_for terms, so can pass result to cursor_for"""
    coll_hof = coll_hof or as_of(mdb.main, datetime.now(tz=timezone.utc))
    if not all(isinstance(line, list) for line in graph_pattern):
        raise ValueError("graph_pattern must be an iterable of lists/tuples")
    expanded_resource = {}
    for line in graph_pattern:
        expanded_line = prefix_expand(line, use_prefixes=use_prefixes)
        for (i, spec), field in zip(enumerate(expanded_line), (E, A, V)):
            if isinstance(spec, str) and not spec.startswith("?"):
                expanded_resource[line[i]] = spec
    # TODO Needs access to full underlying collection, not just filter-extendable cursor,
    #  because may need to create new objectIds for new resources. Should be able to short-circuit
    #  this for the case of queries because resources without objectIds will not satisfy any query
    #  conditions. Low priority, but would enable removal of direct access to collection.
    expanded_resource_oid = _oids_for(
        list(expanded_resource.values()), coll=coll_hof[1]
    )
    oid_for = {
        r: expanded_resource_oid[expanded_resource[r]] for r in expanded_resource.keys()
    }
    out = []
    for line in graph_pattern:
        line_out = [
            (oid_for.get(elt, elt) if isinstance(elt, str) else elt) for elt in line
        ]
        out.append(line_out)
    return out


def cursor_for(condition, coll_hof):
    filter_ = {}
    for spec, field in zip(condition, (E, A, V)):
        if isinstance(spec, dict):
            filter_[field] = list(spec.values())[0]
        elif isinstance(spec, ObjectId):
            filter_[field] = spec
        elif isinstance(spec, str) and not spec.startswith("?"):
            filter_[field] = spec
        elif not isinstance(spec, str):
            raise ValueError(f"Unsupported type {type(spec)} for spec")
    return coll_hof[0](filter_)


def get_doc_binder(condition):
    def doc_binding(doc):
        binding = {}
        doc_vals = (doc[E], doc[A], doc[V])
        for spec, field_val in zip(condition, doc_vals):
            if isinstance(spec, dict):
                spec = list(spec.keys())[0]

            if isinstance(spec, str) and spec.startswith("?"):
                binding[spec] = field_val
            elif spec == field_val:
                continue
            else:
                return None
        return binding if binding else None

    return doc_binding


def merge_binding_collections(bindings1, bindings2):
    merged = []
    for (b1, b2) in itertools.product(bindings1, bindings2):
        no_good = False
        new_binding = copy(b1)
        for k, v in b2.items():
            if k in new_binding and v != new_binding[k]:
                no_good = True
                break
            else:
                new_binding[k] = v
        if no_good:
            continue
        else:
            merged.append(new_binding)
    return merged


def get_valid_bindings(conditions, coll_hof):
    condition_bindings = []
    for c in conditions:
        bindings = []
        doc_binding = get_doc_binder(c)
        for doc in cursor_for(c, coll_hof):
            binding = doc_binding(doc)
            if binding is not None:
                bindings.append(binding)
        condition_bindings.append(bindings)
    valid_bindings = functools.reduce(merge_binding_collections, condition_bindings)
    return valid_bindings


def refs_for(oids, coll_hof=None):
    coll_hof = coll_hof or as_of(mdb.main, datetime.now(tz=timezone.utc))
    out = {}
    docs = list(coll_hof[0]({E: {"$in": oids}, A: {"$in": [OID_URIREF, OID_VAEM_ID]}}))
    for doc in docs:
        if doc[A] == OID_VAEM_ID and doc[E] not in out:
            out[doc[E]] = "_:" + encode_id(doc[V])
        elif doc[A] == OID_URIREF:
            out[doc[E]] = doc[V]
    if len(oids) != len(out):
        missing = set(oids) - set(out)
        raise RuntimeError(
            f"{len(missing)} oids out of {len(oids)} ({missing}) have no refs or IDs"
        )
    return out


def sub_refs(selected, coll_hof=None):
    oid_places = []
    for i, s in enumerate(selected):
        for k, v in s.items():
            if isinstance(v, ObjectId):
                oid_places.append((i, k, v))
    refs = refs_for([o_p[2] for o_p in oid_places], coll_hof=coll_hof)
    out = [s.copy() for s in selected]
    for (i, k, v) in oid_places:
        py_.set_(out[i], k, refs[v])
    return out


def prefix_compact(bindings: List[dict], use_prefixes=None) -> List[dict]:
    prefix = PREFIXES.copy()
    if use_prefixes is not None:
        prefix.update(use_prefixes)
    out = []
    for b in bindings:
        b_new = {}
        for (k, v) in b.items():
            v_new = v
            if isinstance(v, str) and re.match(URI_BEGINNING_PATTERN, v):
                for p, expanded in prefix.items():
                    if v.startswith(expanded):
                        local_name = v.split(expanded, 1)[1]
                        v_new = f"{p}:{local_name}"
                        break
            b_new[k] = v_new
        out.append(b_new)
    return out


def query(query_spec, coll_hof=None):
    """Query data sources.

    :param query_spec: a dictionary with these keys:
      - where: specifies what satisfies this query. Introduces variable names and can use `params`.
      - select: (optional) specifies what is to be returned, using names introduced in `where`.
      - prefixes: (optional) additional prefixes to expand CURIEs used in `where`.
      - params: (optional) names mapping to the provided `args`. [NOT YET IMPLEMENTED]
      - args: (optional) data sources for the query. [NOT YET IMPLEMENTED]

    :param coll_hof: a collection higher-order filter, i.e. the data source for the query.

    The query language notation for use in `where` can be imagined as an unholy reverse-orthology (i.e., a common
    ancestor) of the query forms of MongoDB and Datalog -- its code name is "mongortholog".

    Generate a list of bindings for each condition. A binding is a dictionary mapping variable names to values. Each
    condition will have one binding associated with each compatible (in isolation) datum.

    After all assertion bindings are generated, the cartesian product will yield an iterable of binding-collections that
    will either be unified consistently or discarded. The resulting set of unified bindings is returned, projected to
    the selected variable names.

    """
    if coll_hof is None:
        coll_hof = as_of(mdb.main, datetime.now(tz=timezone.utc))
    conditions = compile_graph_pattern(
        query_spec["where"], use_prefixes=query_spec.get("prefixes"), coll_hof=coll_hof
    )
    valid_bindings = get_valid_bindings(conditions, coll_hof=coll_hof)
    selected_bindings = (
        [py_.pick(v, *query_spec["select"]) for v in valid_bindings]
        if "select" in query_spec
        else valid_bindings
    )
    # TODO compact_with_prefixes after sub_refs and before return
    return prefix_compact(
        sub_refs(selected_bindings, coll_hof=coll_hof),
        use_prefixes=query_spec.get("prefixes"),
    )
