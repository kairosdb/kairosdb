package org.kairosdb.core.blast;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.commons.lang3.RandomUtils;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.KairosDBService;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.core.reporting.KairosMetricReporter;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 Created by bhawkins on 5/16/14.
 */
public class BlastServer implements KairosDBService, Runnable, KairosMetricReporter
{
	public static final Logger logger = LoggerFactory.getLogger(BlastServer.class);

	public static final String NUMBER_OF_ROWS = "kairosdb.blast.number_of_rows";
	public static final String DURATION_SECONDS = "kairosdb.blast.duration_seconds";
	public static final String METRIC_NAME = "kairosdb.blast.metric_name";
	public static final String TTL = "kairosdb.blast.ttl";
	private Thread m_serverThread;
	private final Publisher<DataPointEvent> m_publisher;
	private final LongDataPointFactory m_longDataPointFactory;
	private boolean m_keepRunning = true;
	private final int m_ttl;
	private final int m_numberOfRows;
	private final long m_durration;  //in seconds
	private final String m_metricName;

	private long m_counter = 0L;

	@Inject
	@Named("HOSTNAME")
	private String m_hostName = "none";

	@Inject
	private LongDataPointFactory m_dataPointFactory = new LongDataPointFactoryImpl();

	@Inject
	public BlastServer(FilterEventBus evenBus,
			LongDataPointFactory longDataPointFactory,
			@Named(NUMBER_OF_ROWS) int numberOfRows,
			@Named(DURATION_SECONDS) long durration,
			@Named(METRIC_NAME) String metricName,
			@Named(TTL) int ttl)
	{
		m_publisher = evenBus.createPublisher(DataPointEvent.class);
		m_longDataPointFactory = longDataPointFactory;
		m_ttl = ttl;
		m_numberOfRows = numberOfRows;
		m_durration = durration;
		m_metricName = metricName;
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
	public void run()
	{
		logger.info("Blast Server Running");
		Stopwatch timer = Stopwatch.createStarted();

		while (m_keepRunning)
		{
			long now = System.currentTimeMillis();
			DataPoint dataPoint = m_longDataPointFactory.createDataPoint(now, 42);
			int row = RandomUtils.nextInt(0, m_numberOfRows);
			ImmutableSortedMap<String, String> tags = ImmutableSortedMap.of("row",
					String.valueOf(row), "host", "blast_server");

			DataPointEvent dataPointEvent = new DataPointEvent(m_metricName, tags, dataPoint, m_ttl);
			m_publisher.post(dataPointEvent);
			m_counter ++;

			if ((m_counter % 100000 == 0) && (timer.elapsed(TimeUnit.SECONDS) > m_durration))
				m_keepRunning = false;

		}
	}

	@Override
	public List<DataPointSet> getMetrics(long now)
	{
		ImmutableList.Builder<DataPointSet> ret = ImmutableList.builder();

		DataPointSet ds = new DataPointSet("kairosdb.blast.submission_count");
		ds.addTag("host", m_hostName);
		ds.addDataPoint(m_dataPointFactory.createDataPoint(now, m_counter));
		ret.add(ds);

		return ret.build();
	}
}
