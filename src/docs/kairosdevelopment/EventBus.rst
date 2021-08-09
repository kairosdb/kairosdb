====================
Using The Event Buss
====================

The internal communication of Kairos is mostly done via an event bus.  The event bus
allows you to publish and subscribe to any java object.  The convention is to name classes
passed around the event bus with the Event suffix, like DataPointEvent.  You can find
events that are published by searching for `createPublisher`.  Probably the most
interesting event for plugin developers is the DataPointEvent.  All incoming data
points are passed through the event bus.

-----------------
Publishing events
-----------------

In order to publish events you will need to inject into your code an instance of
`FilterEventBus`.  You will then want to call `createPublisher` and pass in the class
that you will be publishing.

.. code-block:: java

	Publisher<DataPointEvent> publisher = eventBus.createPublisher(DataPointEvent.class);

The publisher that is returned should be a long lived object.


---------------------
Subscribing to events
---------------------

Subscriptions are based on Java class object types.  To subscribe you annotate a single
argument method with the `@Subscribe` annotation.  The argument of the method is the
type of event you want to receive.  Here is an example from `CassandraDatastore.java`

.. code-block:: java

	@Subscribe
	public void putDataPoint(DataPointEvent dataPointEvent) throws DatastoreException

Objects that have the `@Subscribe` annotation and are injected via guice are automatically
registered with the event bus.  If you are creating your own objects outside of guice you can
use an injected instance of `FilterEventBus` to register the object.

Subscriptions are synchronous so if you need to do longer processing on the event then
you should start a new thread to do the work.

Subscription Priority
---------------------

Subscribers to an event are in what is called a pipeline and each subscriber has a
priority in that pipeline.  By default the priority is 50 (1-100).  The data store
subscribes with a priority of 50 so if you want to get the data point before the
data store then set the priority less than 50.  You can set the priority when you register
the object with the `@Subscribe` annotation or you can set the priority in the config
file by using the prefix `kairosdb.eventbus.filter.priority.` and the suffix of the full
class name to the object.

Filtering events
----------------

A subscriber can filter or modify events as they pass through the event pipeline.
To filter or modify the event you simply need to return the same type of object from
your subscriber method as what you are subscribed to.  If your method returns void, the
event bus assumes you do not want to filter/modify the events.  If your method returns the
same type of object the event bus will pass on through the pipeline only what you return.
If your method returns a null it is assumed you have filtered the event and the pipeline stops.

Some examples might be adding tags to events as they come into the system or stopping metrics
from a client that is sending bad data.