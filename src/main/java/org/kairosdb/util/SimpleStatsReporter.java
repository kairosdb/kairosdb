package org.kairosdb.util;

import com.google.inject.name.Named;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;

import javax.inject.Inject;
import java.util.List;

/**
 Created by bhawkins on 1/26/17.
 */
public class SimpleStatsReporter
{
	private final String m_hostName;
	private final LongDataPointFactory m_longDataPointFactory;
	private final DoubleDataPointFactory m_doubleDataPointFactory;

	@Inject
	public SimpleStatsReporter(@Named("HOSTNAME")String hostName,
			LongDataPointFactory longDataPointFactory, DoubleDataPointFactory doubleDataPointFactory)
	{
		m_hostName = hostName;
		m_longDataPointFactory = longDataPointFactory;
		m_doubleDataPointFactory = doubleDataPointFactory;
	}

	public SimpleStatsReporter()
	{
		m_hostName = "localhost";
		m_longDataPointFactory = new LongDataPointFactoryImpl();
		m_doubleDataPointFactory = new DoubleDataPointFactoryImpl();
	}

	private DataPointSet newDataPointSet(String metricPrefix, String metricSuffix)
	{
		DataPointSet dps = new DataPointSet(new StringBuilder(metricPrefix).append(".").append(metricSuffix).toString());
		dps.addTag("host", m_hostName);

		return dps;
	}

	private DataPointSet newDataPointSet(String metricPrefix, String metricSuffix, long now, long value)
	{
		DataPointSet dps = newDataPointSet(metricPrefix, metricSuffix);
		dps.addDataPoint(m_longDataPointFactory.createDataPoint(now, value));

		return dps;
	}

	private DataPointSet newDataPointSet(String metricPrefix, String metricSuffix, long now, double value)
	{
		DataPointSet dps = newDataPointSet(metricPrefix, metricSuffix);
		dps.addDataPoint(m_doubleDataPointFactory.createDataPoint(now, value));

		return dps;
	}


	public void reportStats(SimpleStats.Data stats, long now, String metricPrefix,
			List<DataPointSet> dataPointSets)
	{
		dataPointSets.add(newDataPointSet(metricPrefix, "min", now, stats.min));

		dataPointSets.add(newDataPointSet(metricPrefix, "max", now, stats.max));

		dataPointSets.add(newDataPointSet(metricPrefix, "avg", now, stats.avg));

		dataPointSets.add(newDataPointSet(metricPrefix, "count", now, stats.count));

		dataPointSets.add(newDataPointSet(metricPrefix, "sum", now, stats.sum));
	}

	public void reportStats(SimpleStats.Data stats, long now, String metricPrefix,
			String tagName, String tagValue, List<DataPointSet> dataPointSets)
	{
		DataPointSet dps;

		dps = newDataPointSet(metricPrefix, "min", now, stats.min);
		dps.addTag(tagName, tagValue);
		dataPointSets.add(dps);

		dps = newDataPointSet(metricPrefix, "max", now, stats.max);
		dps.addTag(tagName, tagValue);
		dataPointSets.add(dps);

		dps = newDataPointSet(metricPrefix, "avg", now, stats.avg);
		dps.addTag(tagName, tagValue);
		dataPointSets.add(dps);

		dps = newDataPointSet(metricPrefix, "count", now, stats.count);
		dps.addTag(tagName, tagValue);
		dataPointSets.add(dps);

		dps = newDataPointSet(metricPrefix, "sum", now, stats.sum);
		dps.addTag(tagName, tagValue);
		dataPointSets.add(dps);
	}
}
