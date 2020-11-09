import os

from dotenv import load_dotenv
from pymongo import MongoClient, ASCENDING as ASC, DESCENDING as DESC, IndexModel

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


def create_collection(name, drop=True):
    if drop:
        db.drop_collection(name)
    collection = db[name]
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


if __name__ == "__main__":
    create_collection("main")
