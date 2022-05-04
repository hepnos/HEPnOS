Under the hood
==============

This section explains how HEPnOS organizes its data internally.
It is not necessary to read this to use HEPnOS, and this internal
orhanization is subject to change. This section is targetted at
users who want to have more technical understanding of the inner
workings of HEPnOS.

HEPnOS uses `Yokan <https://github.com/mochi-hpc/mochi-yokan>`_,
Mochi's key/value storage service, to store its data.
A HEPnOS service hence consists of a collection of remotely-accessible
key/value storage databases, each of which will store only one kind
of object (DataSet, Run, SubRun, Event, or Product).

DataSet databases
-----------------

DataSet-storing databases store the full name of the dataset
(e.g. *aaa/bbb/ccc*) as key, prefixed with a single byte that
represents the level of the dataset (e.g. *aaa* has a level
of 0, *aaa/bbb* has a level of 1, *aaa/bbb/ccc* a level of 2,
and so on). These keys are sorted lexicographically. The initial
byte ensures that all the root datasets are listed first, then
the datasets with 1 nesting level, and so on, which simplifies
HEPnOS' iterations over datasets (to list the child datasets
of *aaa/bbb* for instance, we list all the keys that start with
*[2]aaa/bbb*, where *[2]* is a single byte containig the value 2).
This also means that the level of nesting cannot exceed 255.

Each dataset key is associated with a unique UUID value.
This UUID is generated when the dataset is created, and is
used when forming keys for Runs, SubRuns, Events, and Products.

Assuming the HEPnOS service has multiple DataSet databases, this
key/value pair will be stored in one of them, selected by
consistent hashing of the key.

Run databases
-------------

Databases storing Runs use keys formed by concateting
the parent DataSet's UUID and the 64-bit Run's number in
big-endian format. Using DataSet UUIDs rather than the DataSet's
name allows for fixed-size keys (24 bytes). The keys are sorted
lexicographically, which enables easy iterations.
For instance, to list all the Runs in a given DataSet,
we simply query the list of keys that start with that DataSet's UUID.
The fact that the big-endian format is used ensures that the
database can simply sort the keys by looking at its bytes, with
having to reinterpret them as UUID+integer.

These databases do not associate keys with any values
(though they could in the future, e.g. to associate metadata
with runs).

Assuming the HEPnOS service has multiple Run databases,
the database that will store a given Run is determined via
consistent hashing of the UUID part of the key (not the
Run number part).
This leads to all the Runs belonging to the same DataSet ending up in
the same database, which simplifies iterations and search.

SubRun databases
----------------

Databases storing SubRuns use keys formed by concatenating
the parent DataSet's UUID, the parent Run number, and the
SubRun number, both in big-endian format, leading to
fixed-sized keys (32 bytes), sorted lexicographically.

Like for Runs, these databases do not associate keys with any
values.

Assuming the HEPnOS service has multiple SubRun databases,
the database that will store a given SubRun is determined via
consistent hashing of the UUID + Run number part of the key
(but not the SubRun number part).
This leads to all the SubRuns belonging to the same Run ending up
in the same database, which simplifies iterations and search.

Event databases
---------------

Event databases follow the same principle as Run and SubRun
databases, using 40-byte keys formed of the UUID, Run number,
SubRun number, and Event number, sorted, with no associated value.

Assuming the HEPnOS service has multiple Event databases,
the database that will store a given Event is determined via
consistent hashing of the UUID + Run number + SubRun number
part of the key (but not the Event number part).
This leads to all the Events belonging to the same SubRun
ending up in the same database.

Product databases
-----------------

Products are stored in databases with a key of the form
:code:`[item][label]#[type]`, where :code:`[item]` is
a 40-byte representation of its container (UUID + Run number + SubRun number
+ Event number; some of these numbers can be set to InvalidRunNumber,
InvalidSubRunNumber, and InvalidEventNumber respectively if the product
is contained in a DataSet, Run, or SubRun). :code:`[label]` is
the user-provided string label associated with the Product.
:code:`[type]` is the name of the C++ type of the Product.

This database is also sorted lexycographically. Hence all the products
belonging to the same container are next to each other in the database,
and within a given container, products with the same label are also
next to each other.

Assuming the HEPnOS service has multiple Product databases,
the database that will store a given Product is determined via consistent hashing
of the UUID + Run number + Subrun  part of the key (same as for Events).
Hence all the products associated with the same Event, regardless of their
type or label, will be stored in the same database, and all the products associated
with Events in the same SubRun will be stored in the same database.
This strategy is used to improve batching opportunities.
