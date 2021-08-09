package org.kairosdb.eventbus;


import static java.util.Objects.requireNonNull;

public class Publisher<T>
{
	private final Pipeline m_pipeline;

	public Publisher(Pipeline pipeline)
	{
		m_pipeline = requireNonNull(pipeline);
	}

	@SuppressWarnings("unchecked")
	public void post(T event)
	{
		for (FilterSubscriber filterSubscriber : m_pipeline)
		{
			event = (T)filterSubscriber.dispatchEvent(event);
			if (event == null)
			{
				//Event was filtered no need to continue
				break;
			}
		}
	}
}
