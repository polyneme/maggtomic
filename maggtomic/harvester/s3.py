import os
import re
import subprocess
from io import BytesIO

import boto3
from gridfs import GridFS
from pymongo import MongoClient
from toolz import keyfilter
from tqdm import tqdm

AWS_PROFILE = os.getenv("AWS_PROFILE")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX")
MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DBNAME = os.getenv("MONGO_DBNAME")

session = boto3.session.Session(profile_name=AWS_PROFILE)
s3 = session.client("s3")

if os.getenv("MOCK_S3_BUCKET"):
    file = os.getenv("MOCK_S3_BUCKET")
    subprocess.run(
        [
            "mongoimport",
            "--drop",
            "--jsonArray",
            "-d",
            MONGO_DBNAME,
            "-c",
            "s3_list_objects",
            "--host",
            MONGO_HOST,
            "--port",
            str(MONGO_PORT),
            str(file),
        ],
        check=True,
    )

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[MONGO_DBNAME]

FS_COLL_NAME = "s3_object_cache"
fs = GridFS(db, FS_COLL_NAME)
fs_filecoll = db[f"{FS_COLL_NAME}.files"]


def mock_key_ts_map_for(bucket_name=S3_BUCKET, prefix=S3_PREFIX):
    """Using local cache, given S3 bucket name and prefix, return map of {key: timestamp}."""
    doc = db.s3_list_objects.find_one({"bucket": bucket_name, "prefix": prefix})
    return {entry["key"]: entry["ts"] for entry in doc["results"]}


def key_ts_map_for(bucket_name=S3_BUCKET, prefix=S3_PREFIX):
    """Given S3 bucket name and prefix, return map of {key: timestamp}."""
    if os.getenv("MOCK_S3_BUCKET"):
        return mock_key_ts_map_for(bucket_name, prefix)

    results = {}

    def one_round(continuation_token):
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuation_token,
            )
        else:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
            )
        for entry in response["Contents"]:
            results[entry["Key"]] = entry["LastModified"]
        continuation_token = (
            response["IsTruncated"] and response["NextContinuationToken"]
        )
        more = bool(continuation_token)
        return more, continuation_token

    get_more, token = True, False
    while get_more:
        get_more, token = one_round(token)

    return results


def s3_key_value(key, ts, bucket=S3_BUCKET, refresh=False):
    filename = f"{bucket}/{key}"
    last_modified = ts.isoformat()
    if refresh or not fs.exists(filename=filename, last_modified=last_modified):
        f = BytesIO()
        s3.download_fileobj(bucket, key, f)
        fs.put(f.getvalue(), filename=filename, last_modified=last_modified)
    last = fs.get_last_version(filename)
    for past in fs_filecoll.find(
        {"filename": filename, "_id": {"$ne": last._id}}, ["_id"]
    ):
        fs.delete(past["_id"])
    return last


def component_getter_s3(path, bucket=S3_BUCKET, delimiter="/"):
    """Ensure bucket name is part of path, and then return path components."""
    if not path.startswith("s3://"):
        path = "s3://" + bucket + "/" + path
    components = path[len("s3://") :].split(
        delimiter
    )  # XXX py3.9+ only? Use `path.removeprefix("s3://").split(delimiter)` instead.
    return path, components


def get_manifest(pattern=None, pre_fetch=False):
    manifest = key_ts_map_for()
    if pattern:
        manifest = keyfilter(lambda k: re.search(pattern, k), manifest)
    if pre_fetch:
        print("Pre-caching S3 objects...")
        for key, ts in tqdm(list(manifest.items())):
            s3_key_value(key, ts)
    return manifest


class Coordinator:
    """Singleton that fetches from S3 and calls atomizer functions."""

    def __init__(
        self,
        bucket_name,
        prefix,
        atomizer_pattern_groups,
        db_asof_lastrun,
        db_since_lastrun,
    ):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.key_ts_map = {}
        self.atomizer_pattern_groups = atomizer_pattern_groups
        self.db_asof_lastrun = db_asof_lastrun
        self.db_since_lastrun = db_since_lastrun

    def fetch(self, refetch=False):
        if not self.key_ts_map or refetch:
            self.key_ts_map = key_ts_map_for(self.bucket_name, self.prefix)
