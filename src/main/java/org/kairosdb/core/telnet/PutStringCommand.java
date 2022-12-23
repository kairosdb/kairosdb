package org.kairosdb.core.telnet;

import com.google.inject.Inject;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.eventbus.FilterEventBus;

import javax.inject.Named;

import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;

public class PutStringCommand extends PutMillisecondCommand implements TelnetCommand
{
	private final StringDataPointFactory m_stringFactory;

	@Inject
	public PutStringCommand(FilterEventBus eventBus, @Named("HOSTNAME") String hostname,
			LongDataPointFactory longFactory, DoubleDataPointFactory doubleFactory, StringDataPointFactory stringFactory)
	{
		super(eventBus, longFactory, doubleFactory);
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
