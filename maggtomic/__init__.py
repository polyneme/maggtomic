import os
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import List, Tuple, Any, Union

from bson import ObjectId
from dotenv import load_dotenv
from pydash import py_
from pymongo import (
    MongoClient,
    ASCENDING as ASC,
    DESCENDING as DESC,
    IndexModel,
    WriteConcern,
)

from maggtomic.util import generate_id, decode_id

load_dotenv()
MONGO_CONNECTION_URI = os.getenv("MONGO_CONNECTION_URI")
MONGO_DBNAME = os.getenv("MONGO_DBNAME")

client = MongoClient(MONGO_CONNECTION_URI)
db = client[MONGO_DBNAME]

# fields allowed in underlying MongoDB collection
E, A, V, T, O = "e", "a", "v", "t", "o"

INDEX_MODELS = [
    IndexModel(
        [(E, ASC), (A, ASC), (V, ASC), (T, DESC), (O, ASC)], name="EAVT (row/doc)"
    ),
    IndexModel(
        [(A, ASC), (E, ASC), (V, ASC), (T, DESC), (O, ASC)], name="AEVT (column)"
    ),
    IndexModel(
        [(A, ASC), (V, ASC), (E, ASC), (T, DESC), (O, ASC)], name="AVET (key-val)"
    ),
    IndexModel(
        [(V, ASC), (A, ASC), (E, ASC), (T, DESC), (O, ASC)],
        name="VAET (graph)",
        partialFilterExpression={V: {"$type": "objectId"}},
    ),
    IndexModel([(T, DESC)], name="T (history)"),
]

PREFIXES = {
    "qudt": "http://qudt.org/schema/qudt#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "vaem": "http://www.linkedmodel.org/schema/vaem#",
    "prov": "http://www.w3.org/ns/prov#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
}

# Core compact URIs (CURIEs)
CORE_CURIES = ("rdf:resource", "prov:generatedAtTime", "vaem:id", "qudt:value")

# Stable ObjectId to map an arbitrary ObjectId to an RDF URI Reference,
# akin to the use of rdf:resource in XML to map an XML node to RDF URI Reference --
# an internal bridge from MongoDB-land to RDF-land.
OID_URIREF = ObjectId.from_datetime(datetime(1970, 1, 1, 0, tzinfo=timezone.utc))

# Stable ObjectId to map to prov:generatedAtTime, for transaction wall-times.
OID_GENERATED_AT_TIME = ObjectId.from_datetime(
    datetime(1970, 1, 1, 1, tzinfo=timezone.utc)
)

# Stable ObjectId to map to vaem:id, for local (integer-encoded, Crockford Base32) IDs.
# Every database entity should be associated with either a URI reference via rdf:resource
# (preferred -- yay Linked Data!) or a local ID via vaem:id, or both.
OID_VAEM_ID = ObjectId.from_datetime(datetime(1970, 1, 1, 2, tzinfo=timezone.utc))

# Stable ObjectId to map to qudt:value, for use in describing structured values
# (https://www.w3.org/TR/rdf-schema/#ch_value). Every stored statement value should be an ObjectId
# unless the statement attribute is the ObjectId for vaem:id or qudt:value, in which case the
# statement value is an integer (in the case of vaem:id) or is any literal MongoDB-primitive value
# such as a string, datetime, number, or boolean (in the case of qudt:value).
#
# Use of qudt:value rather than e.g. rdf:value encourages qudt:Quantifiable structured values,
# i.e. inclusion of qudt:unit, qudt:standardUncertainty,
# qudt:dataType (qudt:basis, qudt:cardinality, qudt:orderedType, qudt:pythonName, etc.), etc.
# in addition to the qudt:value literal for a structured value.
OID_QUDT_VALUE = ObjectId.from_datetime(datetime(1970, 1, 1, 3, tzinfo=timezone.utc))


def prefix_expand(items: Iterable, use_prefixes=None) -> list:
    prefix = PREFIXES.copy()
    if use_prefixes is not None:
        prefix.update(use_prefixes)
    out = []
    for item in items:
        if isinstance(item, str):
            components = item.split(":", 1)
            if len(components) == 2 and not components[1].startswith("/"):
                pfx, local_name = components
                out.append(f'{prefix.get(pfx, pfx+":")}{local_name}')
            else:
                out.append(item)
        else:
            out.append(item)
    return out


CORE_ATTRIBUTES = {
    curi: expanded for curi, expanded in zip(CORE_CURIES, prefix_expand(CORE_CURIES))
}

LITERAL_VALUED_ATTRIBUTES = frozenset(
    py_.properties("vaem:id", "qudt:value")(CORE_ATTRIBUTES)
)


def create_collection(name, drop_guard=True):
    if drop_guard and name in db.list_collection_names():
        raise ValueError(f"collection `{name}` already exists in db.")
    else:
        db.drop_collection(name)
    collection = db.create_collection(
        name,
        write_concern=WriteConcern(w=1, j=True),
        # TODO schema switch s.t. if attribute not in {objectIdFor(a) for a in {:value,:id,:uriref}},
        #   value must be {bsonType: objectId},
        #   else value has no bsonType restriction.
        #   Possible with JSON Schema?
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["e", "a", "v", "t", "o", "_id"],
                "properties": {
                    "_id": {"bsonType": "objectId"},
                    "e": {"bsonType": "objectId", "title": "entity"},
                    "a": {"bsonType": "objectId", "title": "attribute"},
                    "v": {"title": "value"},
                    "t": {"bsonType": "objectId", "title": "transaction"},
                    "o": {
                        "bsonType": "bool",
                        "title": "operation",
                        "description": "assertion (true) or retraction (false)",
                    },
                },
                "additionalProperties": False,
            }
        },
        # higher compression than default "snappy", lower CPU usage than "zlib".
        storageEngine={"wiredTiger": {"configString": "block_compressor=zstd"}},
    )
    _add_raw(
        [
            (OID_URIREF, OID_URIREF, CORE_ATTRIBUTES["rdf:resource"]),
            (
                OID_GENERATED_AT_TIME,
                OID_URIREF,
                CORE_ATTRIBUTES["prov:generatedAtTime"],
            ),
            (OID_VAEM_ID, OID_URIREF, CORE_ATTRIBUTES["vaem:id"]),
            (OID_QUDT_VALUE, OID_URIREF, CORE_ATTRIBUTES["qudt:value"]),
        ]
    )
    # indexes leverage default prefix compression
    collection.create_indexes(INDEX_MODELS)
    return collection


RawStatement = Tuple[ObjectId, ObjectId, Any]


def _add_raw(raw_statements: List[RawStatement]):
    t = ObjectId()
    docs = [{E: e, A: a, V: v, T: t, O: True} for (e, a, v) in raw_statements]
    docs.append({E: t, A: OID_GENERATED_AT_TIME, V: t.generation_time, T: t, O: True})
    return db.main.insert_many(
        docs
    ).inserted_ids  # raises InvalidOperation if write is unacknowledged


_oids_cache = {}


def oids_for(resources: List[str]) -> List[ObjectId]:
    check_uris(resources)
    docs = [{E: _oids_cache[r], V: r} for r in set(resources) & set(_oids_cache)]
    missing = list(set(resources) - {d[V] for d in docs})
    if missing:  # not in cache? fetch from database.
        docs.extend(list(db.main.find({A: OID_URIREF, V: {"$in": missing}}, [E, V])))
        missing = list(set(resources) - {d[V] for d in docs})
        if missing:  # not in database? add to database.
            new_oids = {r: ObjectId() for r in missing}
            _add_raw([(oid, OID_URIREF, r) for r, oid in new_oids.items()])
            docs.extend([{E: oid, V: r} for r, oid in new_oids.items()])
    for d in docs:
        _oids_cache[d[V]] = d[E]
    return [d[E] for d in docs]


uri_beginning_pattern = re.compile(r"[a-z]\w*?://.")


def check_uris(resources: List[str]) -> List[str]:
    if not all(re.match(uri_beginning_pattern, r) for r in resources):
        raise ValueError("Some resources are not URIs")
    return resources


ExpandedStatement = Tuple[Union[str, ObjectId], Union[str, ObjectId], Any]


def _compile_to_raw(statement: ExpandedStatement) -> RawStatement:
    entity, attribute, value = statement
    objectIds = {c for c in statement if isinstance(c, ObjectId)}
    resources = {
        c
        for c in statement
        if isinstance(c, str) and re.match(uri_beginning_pattern, c)
    }
    non_literals = objectIds | resources
    if not {entity, attribute} <= non_literals:
        raise ValueError(
            "Entity and Attribute must both be non-literals, e.g. (compact) URIs. "
            f"Input statement: {statement}."
        )
    if value not in non_literals and attribute not in LITERAL_VALUED_ATTRIBUTES:
        raise ValueError(
            "Value must be a non-literal, e.g. a (compact) URI, unless attribute is one of "
            f"{{vaem:id, qudt:value}}. Input statement: {statement}."
        )
    rmap = dict(zip(resources, oids_for(list(resources))))
    return (
        rmap.get(entity, entity),
        rmap.get(attribute, attribute),
        rmap.get(value, value),
    )


UserStatement = Tuple[str, str, Any]


def add(statement: UserStatement, use_prefixes=None):
    e_user, a_user, v_user = prefix_expand(statement, use_prefixes=use_prefixes)
    v_user_is_uri = bool(re.match(uri_beginning_pattern, v_user))
    if not v_user_is_uri and a_user not in LITERAL_VALUED_ATTRIBUTES:
        new_oid = ObjectId()
        v_eid = generate_id()
        v_eid_decoded = decode_id(v_eid)
        expanded_statements = [
            (e_user, a_user, new_oid),
            (new_oid, CORE_ATTRIBUTES["qudt:value"], v_user),
            (new_oid, CORE_ATTRIBUTES["vaem:id"], v_eid_decoded),
        ]
    else:
        expanded_statements = [(e_user, a_user, v_user)]
    raw_statements = [_compile_to_raw(s) for s in expanded_statements]
    _add_raw(raw_statements)


# TODO basic CRUD:
#  - register package on pypi.org
#  - "updating" and "deleting" needs to transact retraction statements.
#  - demo use case: insert map of {key: timestamp} as (key, last_updated, timestamp) statements.`


if __name__ == "__main__":
    maincoll = create_collection("main", drop_guard=False)
    add(
        ("vaem:id", "myns:comment", "A shareable ID"),
        use_prefixes={"myns": "scheme://host/ns/mine#"},
    )
