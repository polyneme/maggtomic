import os

from dotenv import load_dotenv
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
                "required": ["e", "a", "v", "t", "o"],
                "properties": {
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
    collection.create_indexes(INDEX_MODELS)
    return collection


# TODO basic CRUD:
#   - create or find objectId for each URI.
#   - arrange for literals (i.e., non-objectId values such as numbers and strings)
#     to be values only for a datom with attribute `qudt:value`.
#     Thus, all values are structured values (https://www.w3.org/TR/rdf-schema/#ch_value).
#     Use of qudt:value rather than e.g. rdf:value supports any qudt:Quantifiable structured value,
#     i.e. inclusion of qudt:unit, qudt:standardUncertainty,
#     qudt:dataType (qudt:basis, qudt:cardinality, qudt:orderedType, qudt:pythonName, etc.), etc.
#     to associate with the qudt:value Literal of a structured value.
#  - "updating" and "deleting" needs to transact retraction statements.
#  - use crockford base32 for user-shareable entity IDs stored as 64-bit integers.


if __name__ == "__main__":
    maincoll = create_collection("main", drop_guard=False)
