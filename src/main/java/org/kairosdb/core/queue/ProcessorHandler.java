package org.kairosdb.core.queue;

import org.kairosdb.events.DataPointEvent;

import java.util.List;

/**
 Created by bhawkins on 10/13/16.
 */
public interface ProcessorHandler
{
	void handleEvents(List<DataPointEvent> events, EventCompletionCallBack eventCompletionCallBack);
}
