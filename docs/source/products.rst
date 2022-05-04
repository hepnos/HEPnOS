Creating and accessing Products
===============================

DataSets, Runs, SubRuns, and Events can store *Products*.
A Product is an instance of any C++ object. Since the mechanism
for storing and loading products is the same when using DataSets,
Runs, SubRuns, and Events, the following code sample illustrates
only how to store products in events.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/08_load_store/main.cpp
       :language: cpp

In this example, we want to store instances of the Particle class.
For this, we need to provide a serialization function for Boost
to use when serializing the object into storage.

We then use the :code:`Event::store()` method to store the
desired object into the event. This method takes a *label* as
a first argument. The pair *(label, product type)* uniquely
addresses a product inside an event. It is not possible to
overwrite an existing product. Hence multiple products of
the same type may be stored in the same event using different
labels. The same label may be used to store products of
different types in the same event.

The second part of the example shows how to use the vector
storage interface. In this example, the :code:`Event::store`
function is used to store a sub-vector of the vector *v*,
from index 1 (included) to index 3 (excluded). The type
of product stored by this way is :code:`std::vector<Particle>`.
Hence it can be reloaded into a :code:`std::vector<Particle>`
later on.
