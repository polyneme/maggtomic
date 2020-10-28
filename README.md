`maggtomic` (*m*etadata *agg*regation using the Da*tomic* information model) is intended to be a
modular system for metadata management.

The primary near-term use case is support of metadata submission, processing, and management for the
[National Microbiome Data Collaborative (NMDC)](https://microbiomedata.org/) pilot project. A priority of
this project is the cultivation of an open (and open-source-powered) data ecosystem.

# Assumptions and Design Considerations

A priority for `maggtomic` is agility. The system must be extensible, with little impedance, to ongoing
introduction of a wide variety of data sources and sinks, all of which need to be findable, accessible,
interoperable, and reusable ([FAIR](https://doi.org/10.1038/sdata.2016.18)).

The [Datomic information model](https://www.infoq.com/articles/Datomic-Information-Model/) facilitates
such agility via its singular, universal relation. It extends the W3C standard
Resource Description Framework ([RDF](https://www.w3.org/TR/2014/REC-rdf11-concepts-20140225/)) information
model, which was designed to facilitate interoperability of distributed data,
with minimal annotation to support ACID transactions. These transactions are reified as durable entities and thus
 may be annotated with provenance, enabling historical auditing and qualified reproducibility. Each and
 every fact-as-of-now (a so-called *datom*) is recorded as a 5-tuple: an RDF triple of entity-attribute-value,
 a transaction id (annotated with
 the transaction wall time as a separate fact), and whether the fact is an assertion or retraction.

To implement the Datomic information model in an open-source system that facilitates agility
for the motivating near-term use case -- supporting the NMDC pilot project -- the most important operational
considerations are familiarity and manageability (see: familiarity) with the chosen technology. `maggtomic`
chooses MongoDB. Why? 
- Much of the infrastructural support for NMDC is located at two U.S. Dept. of Energy (DOE)
user facilities: Joint Genome Institute (JGI) and National Energy Research Scientific Computing Center (NERSC).
The JGI Archive and Metadata Organizer (JAMO), which in turn uses NERSC hardware and staff support,
manages user-facing metadata with MongoDB.
- Other large user-facing facilities use MongoDB for (meta)data management through NERSC,
such as the Advanced Light Source (ALS) user facility and the
Materials Project ([MP](https://materialsproject.org/)).
- Another large project with a focus on biological metadata management, the
Center for Expanded Data Annotation and Retrieval (CEDAR),
uses MongoDB [as a metadata repository](https://doi.org/10.1093/database/baz059).

So, there is strong operational familiarity among relevant stakeholders both at the level of infrastructure
support and of suitability for scientific-domain modeling. But what about system features necessary to
support adequate performance of the Datomic information model? Firstly, `maggtomic` is intended to support
the needs of a pilot project, so *adequate* is an important qualifier on expected performance. Secondly, there
are several features of MongoDB, and choices for configuration, that help address performance concerns:
- *redundant indexing to support a variety of access patterns*: Datomic redundantly stores all data in at
least 4 sort orders, including one index that covers a subset of datoms to support reverse attribute lookup.
For this functionality, MongoDB supports multiple (covering) compound indexes, and partial indexes,
on a collection.
- *compression*: Because (a) all data is stored redundantly in each of several indexes, and (b) all data is
immutable (accumulate-only, great for historical auditing and qualified reproducibility), Datomic index segments
are highly compressed. With the MongoDB (default) WiredTiger storage engine, compression is supported for all
collections and indexes, with different options to trade off higher compression rates versus CPU usage. The
`zstd` library available in MongoDB 4.2 seems appropriate here, with a higher compression rate than the default
`snappy` option and lower CPU usage (and also higher compression rate) than the `zlib` option (previously the
only built-in alternative to `snappy`).
- *transactions*: A performance concern in the sense that losing data is poor performance. MongoDB supports
multi-document transactions as of 4.0 (and across a shared deployment as of 4.2), and configurable write- and
read-concern levels.

There is also the matter of supporting analogues to Datomic schema, query, and
transaction functions, all of which in turn support effective and productive interaction with the underlying
information model. `maggtomic` chooses Python as the language for client interfaces, as this language
is in heavy use by stakeholders.
- For schema support, rather than translate the ad hoc vocabulary used in
Datomic, `maggtomic` aims to support a subset of the RDF-based W3C Shapes Constraint Language
([SHACL](https://www.w3.org/TR/shacl/)) standard, which admittedly was only finalized as a standard in 2017,
whereas Datomic schema was launched earlier. Crucially, Python tooling such as
[pySHACL](https://github.com/RDFLib/pySHACL) exists to validate SHACL shape graphs against data graphs.
- For query, `maggtomic` aims to leverage the expressiveness of the MongoDB query language and the MongoDB
aggregation pipeline to provide a query interface similar in appearance and composability to Datomic's
variant of datalog.
- For transaction functions, `maggtomic` aims to provide Python functions that return e.g. lists of
dictionaries that correspond to tiny MongoDB documents as new datoms, MongoDB aggregation pipeline stages, etc.
Modulo performance considerations, transaction functions or query predicates may be arbitrary Python functions,
as their equivalents may be arbitrary Clojure functions in Datomic, which would manifest e.g. as interruptions
of a MongoDB aggragation pipeline.

Certainly, not all of the above things need to be implemented prior to productive evaluative use of an
alpha version of `maggtomic`, but it's important to consider longer-term ramifications of choosing Python
and MongoDB to implement
([an ad hoc, informally-specified, bug-ridden, slow implementation of half of](https://en.wikipedia.org/wiki/Greenspun%27s_tenth_rule))
the Datomic information model, even if one knows the motivating use case is for agility in the context
of a pilot system and thus one must
[plan to throw one away; you will, anyhow.](https://www.tbray.org/ongoing/When/200x/2008/08/22/Build-One-to-Throw-Away)

Finally, `maggtomic` aims to provide interoperability among data sources and sinks via translation between
JSON-LD serializations (as JSON is a familiar format for stakeholders) and the RDF graphs corresponding
to values of the maggtomic database as-of given times (and thus as a set of entity-attribute-value tuples
for a given filtration of transactions). Again, Python tooling for this translation is crucial, and e.g.
the [pyLD](https://github.com/digitalbazaar/pyld) library is a JSON-LD processor that supports necessary
operations such as expansion+flattening -- context-annotated JSON-LD to RDF -- and
framing(+compacting) -- RDF to context-annotated JSON-LD, which can leverage ontologies installed as
facts-as-of-now themselves in the database.

Dataflow may be handled via "builder" ETL processes as with the Materials Project's
[maggma](https://github.com/materialsproject/maggma) system. Though currently out of scope for the near-term,
it may be possible to construct a [timely dataflow](https://timelydataflow.github.io/timely-dataflow/) system
to support adequately-performant interactive queries of the knowledge graph embodied by a `maggtomic` database.

For Web API support for metadata submission and search/retrieval,
`maggtomic` aims to include a [FastAPI](https://fastapi.tiangolo.com/) server module. For browser-based
metadata submission and basic search/retrieval, `maggtomic` aims to include a (authentication-enabled)
static-site frontend that connects to the Web API.
