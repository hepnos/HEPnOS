Accessing Events
================

Accessing from a SubRun
-----------------------

The example code bellow shows how to create SubRuns inside
Runs, how to iterate over all the SubRuns in a
Run, how to access a SubRun from
a Run, and how to search for SubRuns.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/06_events/main.cpp
       :language: cpp


The SubRun class presents an interface very similar to that
of an :code:`std::map<EventNumber,Event>`, providing users
with :code:`begin` and :code:`end` functions to get forward
iterators, as well as :code:`find`, :code:`lower_bound`, and
:code:`upper_bound` to search for specific Events.
Events are sorted in increasing order of event number.

Accessing from a DataSet
------------------------

Events are stored in SubRuns, hence they can be accessed
from their parent SubRun, as shown above. They can also be
accessed directly from their parent DataSet, providing a
more convenient way of iterating through them without
having to iterate through intermediate Run and SubRun levels.

The following example code shows how to use the
:code:`DataSet::events()` method to get an :code:`EventSet` object.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/07_events_from_dataset/main.cpp
       :language: cpp

The EventSet object is a view of all the Events
inside a give DataSet. It provides :code:`begin` and
:code:`end` methods to iterate over the events.

The :code:`DataSet::events()` method can accept an integer
argument representing a given *target number*. The available
number of targets can be obtained using :code:`DataStore::numTargets()`,
passing :code:`ItemType::EVENT` to indicate that we are interested
in the number of targets that are used for storing events.
Passing such a target number to :code:`DataSet::events()`
will restrict the view of the resulting EventSet to the Events
stored in that target. This feature allows parallel programs
to have distinct processes interact with distinct targets.

Note that the Events in an EventSet are not sorted lexicographically
by (run number, subrun number, event number). Rather, the EventSet
provides a number of guarantees on its ordering of Events:

* In an EventSet restricted to a single target, the Events are
  sorted lexicographically by (run number, subrun number, event number).
* All the Events of a given SubRun are gathered in the same target,
  hence an EventSet restricted to a single target will contain
  *all* the Events of *a subset* of SubRuns of *a subset of Runs*.
* When iterating through an EventSet (whether it is restricted to a specific
  target or not), we are guaranteed to see all the Events of a SubRun before
  another SubRun starts.

In the above sample program, iterating over the global EventSet yields
the same result as iterating over restricted EventSet by increasing
target number.

This EventSet feature can be useful if one wants to have *N* clients
iterate over all the events in a given dataset. Each client can retrieve
events from a single or a subset of targets that way. However, we
encourage the reader to consider using the *ParallelEventProcess* class
in this situation, as it also provides load-balancing across clients.
