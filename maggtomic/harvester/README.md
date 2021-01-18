# Metadata Harvesting

This module is intended to facilitate metadata harvesting, and this document uses the terminology of the [Open Archives
Initiative Protocol for Metadata Harvesting (OAI-PMH)](http://www.openarchives.org/OAI/openarchivesprotocol.html) when
possible.

## Inspiration and Use Cases

In OAI-PMH, a *repository* is a means of exposing metadata to *harvesters*. The OAI-PMH spec goes into great detail
about how a data provider should implement a repository so that a harvester can simply be a client application that
issues one of six possible OAI-PMH requests. However, we often want to harvest metadata from repositories
that...well...they're not OAI-PMH repositories.

This module tries to address harvesting from commonly encountered types of metadata repositories in an OAI-PMH style.
The focus is on delimited-path key-value systems such as object storage, e.g. AWS S3, and file systems, e.g.
SCP-accessible UNIX directories.

An *item* is a unique key within a repository that can yield a metadata *record* about a *resource*. In AWS S3, a bucket
is a repository, a key in that bucket is an item, and the object the key points to is the resource. Using the S3 API,
you can get a metadata record for a key, and the format of that record is customizable -- e.g. you can ask for different
kinds of metadata. For a shared filesystem directory, the directory is the repository, a file path relative to the root
directory is an item, the bytes of a file are a resource, and you can get different metadata records about a file, e.g.
different aggregations of file attributes (last-modified, size, ownership, etc.).

## Path Machines

Importantly, an *item* in the OAI-PMH sense "is conceptually a container that stores or dynamically generates metadata
about a single resource." Using that idea, this module facilitates the definition of
[statecharts](https://statecharts.github.io/) in the form of so-called *path machines*, i.e. finite state machines
(FSMs) that traverse the components of a delimited path (e.g. `bucket->my->meaningful->path->to->something.json` for the
S3 key `s3://bucket/my/meaningful/path/to/something.json`) and maintain extended state (this is partially how
statecharts extend FSMs) to build up a metadata record that is the machine's output.

A path machine also takes as input the timestamp to associate with the record (e.g. the last-modified stamp for the S3
key). In the OAI-PMH spec, a metadata record is identified unambiguously by the combination of three things: the item
identifier, the timestamp of the record, and an identifier for the format of the record. Analogously, a record harvested
by a path machine is identifiable by the (fully-qualified) path, the timestamp, and the id of the path machine used to
generate the record.

## Guidance: Entity-Event Modeling and the PROV Ontology

