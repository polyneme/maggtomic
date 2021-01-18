# Extending the 5-Tuple Model, To a 6- or 7-Tuple Model

The [Datomic information model](https://www.infoq.com/articles/Datomic-Information-Model/) can be seen as
[RDF](https://www.w3.org/TR/2014/REC-rdf11-concepts-20140225/) + transactions + delta-encoding. Each and every
fact-as-of-now (a so-called *datom*) is recorded as a 5-tuple: an RDF triple of subject-predicate-object, a transaction
entity (annotated with the transaction wall time as a separate fact), and a boolean flag indicating whether the triple
is being asserted or retracted in this transaction. In this document, I motivate the introduction of a sixth, and
possibly seventh (no more!) component to a datom.

## Lines and Commits, for One File

Viewed in analogy to version control for code, the triple is like a line of code, a transaction entity is like a commit
that may include several lines, and the assertion/retraction flag is like the marking of each line as being added or
removed, i.e. a delta encoding for the commit. Version control for datoms is dramatically simpler than for code because:
(1) there is only one "file", (2) every "line" has the same triple form, and (3) there is no ordering (apart from
temporal commit order) of these "lines" -- the triples are a set.

In this 5-tuple world, annotation and provenance happens at two levels. The first level is the triple-component level --
you can add triples with any existing subject, predicate, or object entity as the subject of a triple. For example, you
can assert statements about how to interpret and use a predicate/attribute. The second level is the transaction level --
you can add triples with a transaction entity as the subject. For example, you can log the time of the transaction, the
author, and a commit message.

## Comments: Block Docstrings and In-Line Annotation

In code, annotation happens also at levels more granular than transactions, i.e. more granular than the commit level.
Annotation happens also at the level of blocks of code such as modules and functions (e.g. a multi-line docstring), and
even at the level of individual lines (e.g. a comment suffixed to a line of code). These annotations (hopefully!) retain
their meaning across multiple transactions/commits.

It is sometimes argued in the case of code that, because comments are not executable, they are more likely to become
inconsistent over time with the actual code, and thus programmers should strive for "self-documenting" code. However,
annotations relating to provenance, e.g. a DOI reference to the published approach that is being implemented, can be
quite helpful for human interpretation and valorization of the code, and with less risk of becoming inconsistent with
changes.

What if we added a sixth component to a datom, a unique id with which we can address the datom as an entity in triples?
A datom with a second datom's id as subject would be an annotation of that second datom, akin to a comment scoped to a
single line of code. To achieve higher-level annotation akin to module/function docstrings, a set of datom ids could
each be asserted to be in the object position (third component) of a triple with a predicate of `rdfs:member` and a
common subject entity that represents that set of datoms, analogous to a logical block of code. This declared set entity
is like any other entity, and can be further annotated by additional assertions.

Annotation for a set of datoms is less likely to become inconsistent over time than annotation for a block of code. With
code, one needs to *opt out* of associating the same annotation with a block of code when changing part of the code in a
transaction/commit, i.e. the old annotation by default remains associated with the new code. With datoms, however, one
needs to *opt in* to associating the same set entity (and thus its existing annotation) with new datoms by adding these
datoms as members of the set. This is the case even at the equivalent of the "single-line comment" level, as a "change"
to a datom manifests as a new datom with a new id, and thus any annotation for the "old" datom does is not by default
associated with the new datom.

## Multiple Files

A line of code has its place in a single file. It can be moved to a new file in the same code repository, but it always
belongs to one and only one file. An equivalent to a file for a repository of datoms could be a *shard*. What if we add
a seventh component to a datom (this is the last one, I promise!), one that tags a set of datoms -- like a transaction
entity tags a set of datoms -- but that facilitated an across-all-transactions partitioning of datoms akin to the
across-all-commits partitioning of lines of code into files. I expect the principal use case for this additional datom
component would be to shard the data into logical partitions by a "core" entity reflected either (a) in the domain (e.g.
"the shard id is the same as a study id, so that I can quickly fetch all facts about a particular study"), (b) in the
physical distribution of data (e.g. "the shard id is a key to determine which datoms will be stored together on the same
physical server"), or both.

## Related Work

My ideas here were highly motivated by interviews/talks given by [Kevin Feeney and Gavin Mendel-Gleason @
TerminusDB](https://podcast.terminusdb.com/episodes/terminators-on-tech-ep8-remake-anatomy-of-a-knowledge-graph), and by
[Jans Aasman @ Franz Inc](https://www.youtube.com/watch?v=2PbuPyeR5Aw). The interpretation of datoms as a delta-encoding
for a "git for data" was inspired by the framing/marketing of TerminusDB. The practical utility of "triples about
triples" as annotation to facilitate functionality such as "triple-level security" (akin to "cell-level security" in
tabular databases), as well as the utility of a "fourth element of a triple" for sharding, or for a "360 view" of a core
entity, was demonstrated by Aasman in the talk I linked to above.