package org.kairosdb.core.telnet;

import com.google.inject.Inject;
import org.jboss.netty.channel.Channel;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.ValidationException;

import javax.inject.Named;
import java.util.List;

import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class PutStringCommand implements TelnetCommand, KairosMetricReporter
{
	private final String m_hostName;
	private final StringDataPointFactory m_stringFactory;
	private final Publisher<DataPointEvent> m_publisher;

	@Inject
	public PutStringCommand(FilterEventBus eventBus, @Named("HOSTNAME") String hostname,
			StringDataPointFactory stringFactory)
	{
		m_hostName = checkNotNullOrEmpty(hostname);
		m_stringFactory = stringFactory;
		m_publisher = eventBus.createPublisher(DataPointEvent.class);
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		return null;
	}

	@Override
	public void execute(Channel chan, String[] command) throws DatastoreException, ValidationException
	{

	}

	@Override
	public String getCommand()
	{
		return "puts";
	}
}
