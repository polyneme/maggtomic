"""
This module is a direct import of prior exploratory work done by <donny@polyneme.xyz>
on querying RDF data via a subset of SPARQL -- only basic graph pattern (BGP) matching, no filtering --
all using Python-native data structures.

TODO: try adapting this approach for a first stab at querying the MongoDB-backed Datamic information model
  via the nascent mongortholog query language, e.g.
      q = {
        "select": ["?key", "?dt"],
        "where": [
            ["?key", "prov:generatedAtTime", "?dt"],
            {
                "?dt": {
                    "$gt": datetime(2020, 10, 31, tzinfo=timezone.utc),
                    "$lt": datetime(2020, 11, 2, tzinfo=timezone.utc),
                }
            },
        ],
    }
"""

from copy import copy
import functools
import itertools


def prefix_expanded_assertions(query):
    assertions = copy(query["where"])
    # Expand prefixes in any subject, predicate, or object in any assertion
    for i, assertion in enumerate(assertions):
        new_assertion = [None, None, None]

        for j in range(3):
            try:
                prefix, postfix = assertion[j].split(":")
                expansion = query["prefixes"].get(prefix)
                new_assertion[j] = (expansion + postfix) if expansion else assertion[j]
            except ValueError:  # not enough values to unpack
                new_assertion[j] = assertion[j]
                continue
        assertions[i] = tuple(new_assertion)
    return assertions


def get_datum_binder(assertion):
    def datum_binding(datum):
        binding = {}
        for elem_a, elem_d in zip(assertion, datum):
            if elem_a.startswith("?"):
                binding[elem_a] = elem_d
            elif elem_a == elem_d:
                continue
            else:
                return None
        return binding if binding else None

    return datum_binding


def merge_binding_collections(bindings1, bindings2):
    merged = []
    for (b1, b2) in itertools.product(bindings1, bindings2):
        nogood = False
        new_binding = copy(b1)
        for k, v in b2.items():
            if k in new_binding and v != new_binding[k]:
                nogood = True
                break
            else:
                new_binding[k] = v
        if nogood:
            continue
        else:
            merged.append(new_binding)
    return merged


def get_valid_bindings(assertions, rdf_tuples):
    assertion_bindings = []
    for a in assertions:
        bindings = []
        datum_binding = get_datum_binder(a)
        for datum in rdf_tuples:
            binding = datum_binding(datum)
            if binding is not None:
                bindings.append(binding)
        assertion_bindings.append(bindings)
    valid_bindings = functools.reduce(merge_binding_collections, assertion_bindings)
    return valid_bindings


def results(query, rdf_tuples):
    """
    Generate a list of bindings for each assertion.
    A binding is a dictionary mapping variable names to values.
    Each assertion will have one binding associated
    with each compatible (in isolation) RDF datum.

    After all assertion bindings are generated, the cartesian product will yield
    an iterable of binding-collections that will either be unified consistently or discarded.
    The resulting set of unified bindings is returned.
    """
    assertions = prefix_expanded_assertions(query)
    valid_bindings = get_valid_bindings(assertions, rdf_tuples)
    return [
        {k: v for k, v in b.items() if k in query["select"]} for b in valid_bindings
    ]
