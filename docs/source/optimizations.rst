Optimizing accesses
===================

Creating and accessing millions of Runs, SubRuns, or Events
can hace a large performance impact. Hence, multiple optimizations
are available to speed them up.

Batching writes
---------------

The creation of Runs, SubRuns, and Events, as well as the storage
of data products can be batched. The following code sample illustrates
how to use the :code:`WriteBatch` object for this purpose.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/11_batching/main.cpp
       :language: cpp

The WriteBatch object is initialized with a datastore. A second argument,
:code:`unsigned int max_batch_size` (which defaults to 128), can be provided
to indicate that at most this number of operations may be batched together.
When this number of operations have been added to the batch, the batch will
automatically flush its content. The WriteBatch can be flushed manually
using :code:`WriteBatch::flush()`, and any remaining operations will be
flushed automatically when the WriteBatch goes out of scope.

The WriteBatch object can be passed to :code:`DataSet::createRun`,
:code:`Run::createSubRun`, :code:`SubRun::createEvent`, as well
as all the :code:`store` methods.

.. note::
   The :code:`max_batch_size` doesn't represent the total number of items
   that have to be written to trigger a flush. The WriteBatch internally keeps
   as many batches of key/value pairs as the number of underlying databases,
   each batch with its own limit of :code:`max_batch_size`. Hence if
   :code:`max_batch_size` is 128 and the client has written 254 items,
   127 of which will go into one database and 127 other will go into another
   database, the WriteBatch won't automatically flush any of these batches
   until they reach 128.

Prefetching reads
-----------------

Prefetching is a common technique to speed up read accesses. Used alone,
the Prefetcher class will read batches of items when iterating through a
container. The following code sample examplifies its use.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/12_prefetching/main.cpp
       :language: cpp

The Prefetcher object is initialized with a DataStore instance,
and may also be passed a :code:`unsigned int cache_size` and
:code:`unsigned int batch_size`. The cache size is the maximum
number of items that can be prefetched and stored in the prefetcher's cache.
The batch size is the number of items that are requested from the underlying
DataStore in a single operation.

A Prefetcher instance can be passed to most functions from the
RunSet, Run, and SubRun classes that return an iterator. This iterator
will then use the Prefetcher when iterating through the container.
The syntax illustrated above, passing the subrun to the
:code:`Prefetcher::operator()()` method, shows a simple way of enabling
prefetching in a modern C++ style :code:`for` loop.

By default, a Prefetcher will not prefetch products. To enable prefetching
products as well, the :code:`Prefetcher::fetchProduct<T>(label)` can be
used. This method *does NOT load any products*, it tells the Prefetcher to
prefetch products of type T with the specified label as the iteration goes on.
The :code:`load` function that is used to load the product then needs to take
the prefetcher instance as first argument so that it looks in the prefetcher's
cache first rather than in the datastore.

.. important::
   If prefetching is enabled for a given product/label, it is expected
   that the client program consumes the prefetched product by calling
   :code:`load`. If it does not, the prefetcher's memory will fill up
   with prefetched products that are never consumed.

Using asynchronous operations
-----------------------------

Most of the operations on Runs, SubRuns, and Events,
as well as Prefetcher and WriteBatch, can be turned
asynchronous simply by using an :code:`AsyncEngine`
instance. The following code examplifies how.

.. container:: toggle

    .. container:: header

       .. container:: btn btn-info

          main.cpp (show/hide)

    .. literalinclude:: ../../examples/13_async/main.cpp
       :language: cpp

The AsyncEngine object is initialized with a DataStore instance
and a number of threads to spawn. Note that using 0 threads is perfectly
fine since the AsyncEngine turns all communication operations into non-blocking
operations, the lack of background threads will not prevent the AsyncEngine
from being able to make some amount of progress in the background.

The AsyncEngine object can be passed to :code:`DataSet::createRun`,
:code:`Run::createSubRun`, :code:`SubRun::createEvent`, as well
as all the :code:`store` methods. When used, these operations will
be queued in the AsyncEngine and eventually execute in the background.

The AsyncEngine instance can also be passed to the constructor of
WriteBatch and Prefetcher. When used with a WriteBatch, the AsyncEngine
will continually take operations from the WriteBatch, batch them, and
execute them. Hence the batches issued by the AsyncEngine may be smaller
than the maximum batch size of the WritBatch object.

When used with a Prefetcher, the Prefetcher will prefetch asynchronously
using the AsyncEngine's threads.
