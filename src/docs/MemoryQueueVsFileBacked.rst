=================================
Memory Queue vs File Backed Queue
=================================

In the kairosdb.conf file there is a sub section for queue_processor.  The queue
processor class can be set to either MemoryQueueProcessor or FileQueueProcessor.
The primary job of the queue processor is to batch up data so it can be efficiently
sent to whatever data store you have configured.  The queue also helps smooth
out spikes of incoming data.

Regardless of what queue you configure the queue processor works the same.  Data
is first inserted into the queue.  A single worker thread waits for data to be inserted
into the queue.  The thread waits for a minimum batch size or a time limit before
grabbing data to be inserted.  The minimum limits are configured in kairosdb.conf.
A batch of data is then handed off to a pool of ingest threads that send the data
to the configured datastore.

^^^^^^^^^^^^
Memory Queue
^^^^^^^^^^^^
The Memory Queue is a circular queue that is allocated up front and used to store
data points as they come in.  If the memory queue fills up, ingest of data points will block
until there is room in the queue for the additional data.

^^^^^^^^^^^^^^^^^
File Backed Queue
^^^^^^^^^^^^^^^^^
The file backed queue uses both the above mentioned memory queue and a file backed
queue (the bigqueue project that has been forked into the KairosDB git hub project).
Data is always written to both the memory queue and to the file backed queue.
While the memory queue is not full the ingest thread pulls data from the memory queue.
When the memory queue fills up and start overwriting itself the ingest thread switches
to read data from file until it can catch up and switch back to the memory queue.

The file backed queue is a bit slower as it writes data to both locations it offeres
two advantages.

1. Almost unlimited queueing (depending on disk space)
2. Data recovery in case of a crash.  Upon startup if there is data in the file
   backed queue it is read first and sent to the backend.

There are several metrics that help diagnose problems with the queue.

* kairosdb.queue.file_queue.size - size of queue on file.
* kairosdb.queue.read_from_file - count of data points read from file instead of memory (indicator that the memory queue has filled up).
* kairosdb.queue.process_count - number of data points read from queue

You may be wondering how useful these metircs are if the queue is full.  The above
metrics are special in that they get to jump to the head of the line when the ingest
thread reads from the queue, so no matter how far behind the queue is these metrics will
always be up to date.