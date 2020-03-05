Concepts and data organization
==============================

HEPnOS handles data in a hierarchy of DataSets, Runs, SubRuns, and Events.
Each of these constructs can be used to store data objects, or Products.

DataSets
--------

**DataSets** are named containers. They can contain other DataSets,
as well as Runs. DataSet can be seen as the equivalent of file system
directories. While HEPnOS enables iterating over the DataSets stored in
a parent DataSet, it has not been deesigned to efficiently handle a large
number of them. Operations on a DataSet include creating a child DataSet,
creating a Run, iterating over the child DataSets, iterating over Runs,
searching for child DataSets by name and child Runs by run number.

Runs
----

**Runs** are numbered containers. They are identified by an integer between
0 and *InvalidRunNumber*, and can contain only SubRuns. Operations on a Run
include creating and accessing individual SubRuns, iterating over SubRuns,
and searching for specific SubRuns.

SubRuns
-------

**SubRuns** are numbered containers. They are identified by an integer
between 0 and *InvalidSubRunNumber*-1, and can contain only Events.
Operations on a SubRun include creating and accessing individual Events,
iterating over events, and searching for specific Events.

Events
------

**Events** are numbered containers. They are identified by an integer
between 0 and *InvalidEventNumber*-1. They may only be used to store
and load Products.

Products
--------

**Products** are *key/value* pairs where the *key* is formed of a string
label and the C++ type of the *value* object, while *value* is the data from
the stored C++ object. While Products can be stored in DataSets, Runs, SubRuns,
and Events, they are typically only stored in Events.


As the only type of named container, DataSets are a convenient way of
naming data coming out of an experiment or a step in a workflow.
Runs, SubRuns, and Events are stored in a way that optimizes search and
iterability in a distributed manner. A DataSet can be expected to store
a large number of runs themselves containing a large number of subruns
and ultimately events.
Products are stored in a way that does not make them iterable. It is
not possible, from a container, to list the contained Products. The
label and C++ type of a Product have to be known in order to retrieve
the corresponding Product data from a container.
