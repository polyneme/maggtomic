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
from pymongo.collection import Collection
from pymongo.errors import WriteError

from maggtomic.util import generate_id, decode_id

load_dotenv()
MONGO_CONNECTION_URI = os.getenv("MONGO_CONNECTION_URI")
if MONGO_CONNECTION_URI is None:
    MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
    MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
    MONGO_CONNECTION_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
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
OID_URIREF = ObjectId.from_datetime(datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc))

# Stable ObjectId to map to prov:generatedAtTime, for transaction wall-times.
OID_GENERATED_AT_TIME = ObjectId.from_datetime(
    datetime(1970, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
)

# Stable ObjectId to map to vaem:id, for local (integer-encoded, Crockford Base32) IDs.
# Every database entity should be associated with either a URI reference via rdf:resource
# (preferred -- yay Linked Data!) or a local ID via vaem:id, or both.
OID_VAEM_ID = ObjectId.from_datetime(datetime(1970, 1, 1, 0, 0, 2, tzinfo=timezone.utc))

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
OID_QUDT_VALUE = ObjectId.from_datetime(
    datetime(1970, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
)


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

_oids_cache = {}


def create_collection(name="main", drop_guard=True):
    if drop_guard and name in db.list_collection_names():
        raise ValueError(f"collection `{name}` already exists in db.")
    else:
        db.drop_collection(name)
        _oids_cache.clear()
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
    _assert_raw(
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
RawStatementOperation = Tuple[ObjectId, ObjectId, Any, bool]


def generate_id_unique(coll: Collection = None, **generate_id_kwargs) -> str:
    coll = coll or db.main
    get_one = True
    while get_one:
        eid = generate_id(**generate_id_kwargs)
        eid_decoded = decode_id(eid)
        get_one = coll.count_documents({A: OID_VAEM_ID, V: eid_decoded}) > 0
    return eid


def _transact_raw(
    raw_statement_operations: List[RawStatementOperation], coll: Collection = None
):
    coll = coll or db.main
    t = ObjectId()
    t_eid = generate_id_unique(coll=coll)
    t_eid_decoded = decode_id(t_eid)
    docs = [{E: e, A: a, V: v, T: t, O: o} for (e, a, v, o) in raw_statement_operations]
    docs.extend(
        [
            {E: t, A: OID_GENERATED_AT_TIME, V: t.generation_time, T: t, O: True},
            {E: t, A: OID_VAEM_ID, V: t_eid_decoded, T: t, O: True},
        ]
    )
    # TODO idempotent assert/retract, i.e. don't re-state datoms.
    inserted_ids = coll.insert_many(
        docs
    ).inserted_ids  # raises InvalidOperation if write is unacknowledged
    if len(inserted_ids) != len(docs):
        raise WriteError("not all documents inserted for transaction")
    return inserted_ids


def _assert_raw(raw_statements: List[RawStatement], coll: Collection = None):
    _transact_raw([(e, a, v, True) for (e, a, v) in raw_statements], coll=coll)


def _oids_for(resources: List[str], coll: Collection = None) -> List[ObjectId]:
    check_uris(resources)
    docs = [{E: _oids_cache[r], V: r} for r in set(resources) & set(_oids_cache)]
    missing = list(set(resources) - {d[V] for d in docs})
    if missing:  # not in cache? fetch from database.
        docs.extend(list(db.main.find({A: OID_URIREF, V: {"$in": missing}}, [E, V])))
        missing = list(set(resources) - {d[V] for d in docs})
        if missing:  # not in database? add to database.
            new_oids = {r: ObjectId() for r in missing}
            _assert_raw(
                [(oid, OID_URIREF, r) for r, oid in new_oids.items()], coll=coll
            )
            docs.extend([{E: oid, V: r} for r, oid in new_oids.items()])
    for d in docs:
        _oids_cache[d[V]] = d[E]
    return {d[V]: d[E] for d in docs}


URI_BEGINNING_PATTERN = re.compile(r"[a-z]\w*?://.")


def check_uris(resources: List[str]) -> List[str]:
    if not all(re.match(URI_BEGINNING_PATTERN, r) for r in resources):
        raise ValueError("Some resources are not URIs")
    return resources


ExpandedStatement = Tuple[Union[str, ObjectId], Union[str, ObjectId], Any]


def _compile_to_raw(
    statement: ExpandedStatement, coll: Collection = None
) -> RawStatement:
    entity, attribute, value = statement
    objectIds = {c for c in statement if isinstance(c, ObjectId)}
    resources = {
        c
        for c in statement
        if isinstance(c, str) and re.match(URI_BEGINNING_PATTERN, c)
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
    rmap = _oids_for(resources, coll=coll)
    return (
        rmap.get(entity, entity),
        rmap.get(attribute, attribute),
        rmap.get(value, value),
    )


UserStatement = Tuple[str, str, Any]


def _ensure_structured_literal(
    statement: UserStatement, use_prefixes=None, coll: Collection = None
) -> List[ExpandedStatement]:
    e_user, a_user, v_user = prefix_expand(statement, use_prefixes=use_prefixes)
    v_user_is_uri = isinstance(v_user, str) and re.match(URI_BEGINNING_PATTERN, v_user)
    if not v_user_is_uri and a_user not in LITERAL_VALUED_ATTRIBUTES:
        new_oid = ObjectId()
        v_eid = generate_id_unique(coll=coll)
        v_eid_decoded = decode_id(v_eid)
        expanded_statements = [
            (e_user, a_user, new_oid),
            (new_oid, CORE_ATTRIBUTES["qudt:value"], v_user),
            (new_oid, CORE_ATTRIBUTES["vaem:id"], v_eid_decoded),
        ]
    else:
        expanded_statements = [(e_user, a_user, v_user)]
    return expanded_statements


def assert_(
    statement: UserStatement, use_prefixes=None, coll: Collection = None
) -> List[RawStatementOperation]:
    return assert_or_retract(
        statement, is_assert=True, use_prefixes=use_prefixes, coll=coll
    )


def retract(
    statement: UserStatement, use_prefixes=None, coll: Collection = None
) -> List[RawStatementOperation]:
    return assert_or_retract(
        statement, is_assert=False, use_prefixes=use_prefixes, coll=coll
    )


def assert_or_retract(
    statement: UserStatement, is_assert=True, use_prefixes=None, coll: Collection = None
) -> List[RawStatementOperation]:
    expanded_statements = _ensure_structured_literal(
        statement, use_prefixes=use_prefixes, coll=coll
    )
    raw_statements = [_compile_to_raw(s, coll=coll) for s in expanded_statements]
    return [(e, a, v, is_assert) for (e, a, v) in raw_statements]


def transact(rso_sequence: List[List[RawStatementOperation]], coll: Collection = None):
    _transact_raw(py_.flatten(rso_sequence), coll=coll)


def as_of(coll: Collection, t: Union[ObjectId, datetime]):
    """Returns a higher-order filter that can produce a collection cursor.

    A higher-order filter for collection coll is a function that, when passed a filter F, combines a previously
    specified filter (based on the value t given to as_of) with the filter F, and returns a cursor over the collection
    coll using the combined filter.

    """
    if isinstance(t, datetime):
        oid = coll.find_one(
            {A: OID_GENERATED_AT_TIME, V: {"$lte": t}}, [E], sort=[(T, DESC)]
        )[E]
    else:
        oid = t

    def docs_for(filter_):
        py_.set_(filter_, [T, "$lte"], oid)
        return coll.find(filter_)

    return docs_for, coll


# TODO basic CRUD
#  or rather, "ARAR" (pirate voice): create->assert, read->read, update->accumulate, delete->retract.
#  - idempotent assert/retract, i.e. don't re-state datoms.
#  - "update"
#    - accumulate for cardinality/many
#    - "replace" for cardinality/one, i.e. retract and assert.
#    - "replace iff" for cardinality/one, i.e. compare-and-swap (CAS).
#  - "delete", i.e. retract


if __name__ == "__main__":
    from maggtomic.query import query

    mycoll = create_collection(drop_guard=False)

    additional_prefixes = {"myns": "s://host/ns/myns#", "s": "http://schema.org/"}
    key_time_statements = [
        (
            f"myns:key{n:02}",
            "s:dateModified",
            datetime(2020, 11, 1, tzinfo=timezone.utc),
        )
        for n in range(20)
    ]
    transact(
        [assert_(s, use_prefixes=additional_prefixes) for s in key_time_statements]
    )

    query_spec = {
        "prefixes": additional_prefixes,
        "select": ["?key", "?dt"],
        "where": [
            ["?key", "s:dateModified", "?sv"],
            [
                "?sv",
                "qudt:value",
                {
                    "?dt": {
                        "$gt": datetime(2020, 10, 31, tzinfo=timezone.utc),
                        "$lt": datetime(2020, 11, 2, tzinfo=timezone.utc),
                    }
                },
            ],
        ],
    }
    coll_as_of_now = as_of(mycoll, datetime.now(tz=timezone.utc))
    results = query(query_spec, coll_hof=coll_as_of_now)
    assert len(results) == len(key_time_statements)
    assert all(k.startswith("myns:") for k in py_.pluck(results, "?key"))
