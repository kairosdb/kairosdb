package org.kairosdb.core.telnet;

import com.google.inject.Inject;
import org.jboss.netty.channel.Channel;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.kairosdb.util.ValidationException;

import javax.inject.Named;
import java.util.List;

import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;

public class PutStringCommand extends PutMillisecondCommand implements TelnetCommand
{
	private final StringDataPointFactory m_stringFactory;

	@Inject
	public PutStringCommand(FilterEventBus eventBus, @Named("HOSTNAME") String hostname,
			LongDataPointFactory longFactory, DoubleDataPointFactory doubleFactory, StringDataPointFactory stringFactory)
	{
		super(eventBus, hostname, longFactory, doubleFactory);
		m_stringFactory = stringFactory;
	}

	@Override
	protected DataPoint createDataPoint(long timestamp, String value)
	{
		return m_stringFactory.createDataPoint(timestamp, value);
	}

	@Override
	public String getCommand()
	{
		return "puts";
	}
}
