import os
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import List, Tuple, Any

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

# Stable ObjectId to map an arbitrary ObjectId to an RDF URI Reference,
# i.e. `OID_URIREF rdfs:domain ObjectId ; rdfs:range rdfs:Resource .`,
# an internal bridge from MongoDB-land to RDF-land.
OID_URIREF = ObjectId.from_datetime(datetime(1970, 1, 1, 0, tzinfo=timezone.utc))

# Stable ObjectId to map to <http://www.w3.org/ns/prov#generatedAtTime>, for transaction wall-times.
GENERATED_AT_TIME = ObjectId.from_datetime(datetime(1970, 1, 1, 1, tzinfo=timezone.utc))

PREFIXES = {
    "qudt": "http://qudt.org/schema/qudt#",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "vaem": "http://www.linkedmodel.org/schema/vaem#",
    "prov": "http://www.w3.org/ns/prov#",
}


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


CORE_CURIS = ("qudt:value", "rdf:resource", "vaem:id", "prov:generatedAtTime")
CORE_ATTRIBUTES = {
    curi: expanded for curi, expanded in zip(CORE_CURIS, prefix_expand(CORE_CURIS))
}


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
    )
    _add_raw(
        [
            (OID_URIREF, OID_URIREF, CORE_ATTRIBUTES["rdf:resource"]),
            (GENERATED_AT_TIME, OID_URIREF, CORE_ATTRIBUTES["prov:generatedAtTime"]),
        ]
    )
    oids_for(py_.properties("vaem:id", "qudt:value")(CORE_ATTRIBUTES))
    collection.create_indexes(INDEX_MODELS)
    return collection


RawStatement = Tuple[ObjectId, ObjectId, Any]


def _add_raw(raw_statements: List[RawStatement]):
    t = ObjectId()
    docs = [{E: e, A: a, V: v, T: t, O: True} for (e, a, v) in raw_statements]
    docs.append({E: t, A: GENERATED_AT_TIME, V: t.generation_time, T: t, O: True})
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


Statement = Tuple[str, str, Any]


def _compile_to_raw(statement: Statement) -> RawStatement:
    entity, attribute, value = statement
    resources = {c for c in statement if re.match(uri_beginning_pattern, c)}
    if entity not in resources or attribute not in resources:
        raise ValueError(
            "Entity and Attribute must both be resources, i.e. (prefixed) URIs."
        )
    if value not in resources and attribute not in py_.properties(
        "vaem:id", "qudt:value"
    )(CORE_ATTRIBUTES):
        raise ValueError(
            "Value must be a resource, i.e. a (prefixed) URI, unless attribute is one of "
            "{vaem:id, qudt:value}."
        )
    rmap = dict(zip(resources, oids_for(list(resources))))
    return rmap[entity], rmap[attribute], rmap.get(value, value)


Fact = Tuple[str, str, Any]


def add(fact: Fact):
    statement = prefix_expand(fact)
    # TODO If value isn't URI, first generate URI for new structured value (using crockford base32 ID for
    #   last part of URI), then _compile_to_raw both
    #   (e_user, a_user, new_uri) and (new_uri, qudt:value, v_user) statements,
    #   and finally _add_raw both together.
    pass


# TODO basic CRUD:
#  - use crockford base32 for user-shareable entity IDs stored as 64-bit integers.
#  - arrange for literals (i.e., non-objectId values such as numbers and strings)
#     to be values only for a datom with attributes in
#     {objectId(r) for r in {"vaem:id", "qudt:value"}}.
#     Thus, most values are structured values (https://www.w3.org/TR/rdf-schema/#ch_value).
#     Use of qudt:value rather than e.g. rdf:value supports any qudt:Quantifiable structured value,
#     i.e. inclusion of qudt:unit, qudt:standardUncertainty,
#     qudt:dataType (qudt:basis, qudt:cardinality, qudt:orderedType, qudt:pythonName, etc.), etc.
#     to associate with the qudt:value Literal of a structured value.
#  - default to zstd compression
#  - add ref to Linda / tuple spaces to README
#  - register package on pypi.org
#  - "updating" and "deleting" needs to transact retraction statements.
#  - demo use case: insert map of {key: timestamp} as (key, last_updated, timestamp) statements.`


if __name__ == "__main__":
    maincoll = create_collection("main", drop_guard=False)
