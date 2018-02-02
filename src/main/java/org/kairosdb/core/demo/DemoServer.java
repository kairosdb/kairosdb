package org.kairosdb.core.demo;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import org.joda.time.DateTime;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.List;

public class DemoServer implements KairosDBService, Runnable, KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(DemoServer.class);

	public static final String METRIC_NAME = "kairosdb.demo.metric_name";
	public static final String NUMBER_OF_ROWS = "kairosdb.demo.number_of_rows";
	public static final String TTL = "kairosdb.demo.ttl";
	private final String m_metricName;
	private final Publisher<DataPointEvent> m_publisher;
	private final DoubleDataPointFactory m_doubleDataPointFactory;
	private final int m_numberOfRows;
	private final int m_ttl;
	private final LongDataPointFactory m_longDataPointFactory;

	private Thread m_serverThread;
	private boolean m_keepRunning = true;
	private long m_counter = 0L;


	@javax.inject.Inject
	@Named("HOSTNAME")
	private String m_hostName = "none";

	@Inject
	public DemoServer(
			@Named(METRIC_NAME) String metricName,
			FilterEventBus eventBus,
			DoubleDataPointFactory doubleDataPointFactory,
			LongDataPointFactory longDataPointFactory,
			@Named(NUMBER_OF_ROWS) int numberOfRows,
			@Named(TTL) int ttl
			)
	{
		m_metricName = metricName;
		m_publisher = eventBus.createPublisher(DataPointEvent.class);
		m_doubleDataPointFactory = doubleDataPointFactory;
		m_longDataPointFactory = longDataPointFactory;
		m_numberOfRows = numberOfRows;
		m_ttl = ttl;
	}

	@Override
	public void run()
	{
		logger.info("Loading one year of demo data...");

		long now = new DateTime().getMillis();
		long insertTime = new DateTime().minusDays(365).getMillis();
		long startTime = insertTime;
		double period = 86400000.0;

		Stopwatch timer = Stopwatch.createStarted();

		while (m_keepRunning && insertTime < now)
		{
			for (int I = 0; I < m_numberOfRows; I++)
			{
				double value = ((double)(insertTime-startTime) / period) + ((double)I / (double)m_numberOfRows);
				//System.out.println(value);
				value = Math.sin(value * 2.0 * Math.PI);
				//System.out.println(value);
				DataPoint dataPoint = m_doubleDataPointFactory.createDataPoint(insertTime, value);
				ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("host", "demo_server_"+I);
				DataPointEvent dataPointEvent = new DataPointEvent(m_metricName, tags, dataPoint, m_ttl);
				m_publisher.post(dataPointEvent);
				m_counter++;
			}

			insertTime += 60000; //Advance 1 minute
		}
	}

	@Override
	public void start() throws KairosDBException
	{
		m_serverThread = new Thread(this);
		m_serverThread.start();
	}

	@Override
	public void stop()
	{
		m_keepRunning = false;
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		ImmutableList.Builder<DataPointSet> ret = ImmutableList.builder();

		DataPointSet ds = new DataPointSet("kairosdb.demo.submission_count");
		ds.addTag("host", m_hostName);
		ds.addDataPoint(m_longDataPointFactory.createDataPoint(now, m_counter));
		ret.add(ds);

		return ret.build();
	}
}
