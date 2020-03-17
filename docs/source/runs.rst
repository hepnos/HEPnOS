Accessing Runs
==============

The example code bellow show how to create Runs inside
DataSets, how to iterate over all the runs in a
DataSet, how to access a Run from
a parent DataSet, and how to search for Runs.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/04_runs/main.cpp
       :language: cpp

The Runs in a DataSets can be accessed using the :code:`DataSet::runs()`
method, which produces a :code:`RunSet` object. A :code:`RunSet` is
a view of the DataSet for the purpose of accessing Runs.

The RunSet class presents an interface very similar to that
of an :code:`std::map<RunNumber,Run>`, providing users
with :code:`begin` and :code:`end` functions to get forward
iterators, as well as :code:`find`, :code:`lower_bound`, and
:code:`upper_bound` to search for specific Runs.
Runs are sorted in increasing order of run number.
