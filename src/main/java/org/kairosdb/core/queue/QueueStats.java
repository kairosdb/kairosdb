package org.kairosdb.core.queue;

import org.kairosdb.metrics4j.annotation.Help;
import org.kairosdb.metrics4j.annotation.Key;
import org.kairosdb.metrics4j.collectors.LongCollector;

public interface QueueStats
{
	//kairosdb.queue.read_from_file
	@Help("If metric consumption has lagged behind to the point the queue is reading from disk this value will be non zero")
	LongCollector readFromFile();

	@Help("Number of metrics processed by the queue")
	//kairosdb.queue.process_count
	LongCollector processCount(@Key("queue")String queue);

	LongCollector batchStats();
}
