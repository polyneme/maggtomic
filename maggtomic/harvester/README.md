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
about a single resource." Using that idea, this module facilitates the definition of statecharts
([spec](https://www.w3.org/TR/scxml/), [intro](https://statecharts.github.io/)) in the form of so-called *path
machines*, i.e. finite state machines (FSMs) that traverse the components of a delimited path (e.g.
`bucket->my->meaningful->path->to->something.json` for the S3 key `s3://bucket/my/meaningful/path/to/something.json`)
and maintain extended state (this is partially how statecharts extend FSMs) to build up a metadata record that is the
machine's output.

A path machine also takes as input the timestamp to associate with the record (e.g. the last-modified stamp for the S3
key). In the OAI-PMH spec, a metadata record is identified unambiguously by the combination of three things: the item
identifier, the timestamp of the record, and an identifier for the format of the record. Analogously, a record harvested
by a path machine is identifiable by the (fully-qualified) path, the timestamp, and the id of the path machine used to
generate the record.

## Guidance for Path Machines: Subject-Event Modeling and the PROV Ontology

The RDF data model is quite flexible: Anybody can say Anything about Any topic (aka the "AAA slogan"). However, we
recommend a particular modeling strategy when it comes to entering new facts into the system. Once entered this way,
analysis workflows may add additional derived facts in whatever form is most suitable. This strategy has been called
*entity-event modeling* [in the context of RDF graphs](https://www.youtube.com/watch?v=2PbuPyeR5Aw), and is a
specialization of the broader [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html) pattern.

First, identify one or two "core" subject types for your domain. These core subjects should be suitable as the principal
subjects across applications -- they are domain-specific, not application-specific. It helps for this decision to
clarify the distinction between a "subject" and an event, and this is where the W3C provenance
([PROV](https://www.w3.org/TR/2013/REC-prov-dm-20130430/)) data model, a vocabulary with a [mapping to
RDF](https://www.w3.org/TR/2013/REC-prov-o-20130430/), comes in.

At its core, PROV describes the use and production of *entities* by *activities*, which may be influenced in various
ways by *agents* (such as people). Crucially, the distinction between activity and entity is similar to that between
['continuant' and 'occurrent', respectively, in logic](http://www.ditext.com/johnson/intro-3.html). A `prov:Activity`
may be a subject of ongoing concern: generating, using, and invalidating entities; being triggered to start or end by
entities; associating with agents (that in turn may have roles or plans, or may delegate to other agents); and
communicating with other activities via shared entities.

However, depending on your domain, your "core" subject  may be any of `prov:Activity`, `prov:Agent` or even
`prov:Entity`. Think about the most important set of "continuing" subjects across many instantaneous occurrences, i.e.
observations of state. For a [multi-omics data collaborative](https://microbiomedata.org/), the core subject may be a
study (a `prov:Activity`). For [a collection of data on all known inorganic materials](https://materialsproject.org/),
the core subject may be a "material" (a `prov:Entity` with many specializations, i.e. alternate structures). For many
service businesses, the core subject may be a customer or patient (a `prov:Agent`).

### Case Study: `prov:Activity`-`prov:qualifiedInfluence` as Subject-Event

In the case of a prov:Activity as "core subject", we could refer separately to a prov:Plan (which is rdfs:subClassOf
prov:Entity) that represents, as stated in the PROV-O docs, "a set of actions or steps intended by one or more agents to
achieve some goals," i.e. the template for the "run-of-a-template" that is the core prov:Activity. More concretely, a
prov:Activity may have a prov:qualifiedAssociation with a prov:agent that prov:hadPlan such a prov:Plan.

It is crucial that each "event" have a time, i.e. prov:atTime. A prov:InstantaneousEvent has prov:atTime, and has
subclasses prov:Start, prov:Generation, prov:Usage, prov:Invalidation, and prov:End. A prov:qualifiedInfluence is not
necessarily a prov:InstantaneousEvent, and thus e.g. the prov:qualifiedAssociation and prov:qualifiedCommunication
relations for a subject activity needn't necessarily have a prov:Time. This puts a wrinkle in using
prov:qualifiedInfluence relations as the reified "event" for a subject-event model, but if we stipulate that, in our
data model, a prov:qualifiedInfluence has rdfs:range prov:InstantaneousEvent, then we are saying that we expect a
prov:atTime for each prov:qualifiedInfluence.

Thus, an example "subject-event" (i.e. `prov:activity`-`prov:qualifiedInfluence`) tree:
```python
{"@id":  myStudy,
 "prov:qualifiedUsage": {
     "prov:entity": {
         "@id": s3key_vXYZ,
         "prov:specializationOf": s3key
     },
     "prov:atTime": {
         "qudt:value": "2012-04-12T00:00:00-04:00",
         "rdf:type": "xsd:dateTime"
     }
 }
}
```

## Additional Metadata from Atomized Resources, via a Rule Engine

After applying path machines to each found path item in a repository, we have a little metadata store separate from our
main one. We can then supply a set of rules to harvest additional metadata based on what resources are newly of
interest. We can fetch resources (i.e. objects and files) to which path items point, "atomize" them based on their MIME
type (e.g. convert application/json and text/csv data to datoms), and use a [rule
engine](https://en.wikipedia.org/wiki/Rule-based_system) to coordinate the construction of new facts based on our
accumulating metadata store (and of course we can consult our main store here as well) until no rules need to fire
again.

The rules should not be computationally intensive -- this is not about deriving new resources, it is about accumulating
relevant metadata that requires opening and parsing the given resources. After we push our new collection of metadata to
the main store, the harvester is done. Performing workflows across our core subjects to further enrich our metadata is a
separate and subsequent activity.