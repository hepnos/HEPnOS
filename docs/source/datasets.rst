Accessing DataSets
==================

The example code bellow shows how to create DataSets inside other
DataSets, how to iterate over all the child datasets of a parent
DataSet, how to access a DataSet using an "absolute path" from
a parent DataSet, and how to search for DataSets.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/03_datasets/main.cpp
       :language: cpp


The DataSet class presents an interface very similar to that
of an :code:`std::map<std::string,DataSet>`, providing users
with :code:`begin` and :code:`end` functions to get forward
iterators, as well as :code:`find`, :code:`lower_bound`, and
:code:`upper_bound` to search for DataSets.
DataSets are sorted in alphabetical order when iterating.
