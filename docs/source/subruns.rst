Accessing SubRuns
=================

The example code bellow shows how to create SubRuns inside
Runs, how to iterate over all the SubRuns in a
Run, how to access a SubRun from
a Run, and how to search for SubRuns.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/05_subruns/main.cpp
       :language: cpp

The Run class presents an interface very similar to that
of an :code:`std::map<SubRunNumber,SubRun>`, providing users
with :code:`begin` and :code:`end` functions to get forward
iterators, as well as :code:`find`, :code:`lower_bound`, and
:code:`upper_bound` to search for specific SubRuns.
SubRuns are sorted in increasing order of subrun number.
