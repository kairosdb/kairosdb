/*
 * Copyright 2013 Proofpoint Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kairosdb.core.reporting;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.DataPointSet;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.exception.DatastoreException;

import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 9/4/13
 Time: 2:56 PM
 To change this template use File | Settings | File Templates.
 */
public class ThreadReporter
{
	private static class ReporterDataPoint
	{
		private String m_metricName;
		private long m_value;
		private SortedMap<String, String> m_tags;

		private ReporterDataPoint(String metricName, SortedMap<String, String> tags, long value)
		{
			m_metricName = metricName;
			m_value = value;
			m_tags = new TreeMap<String, String>(tags);
		}

		public String getMetricName() { return (m_metricName); }
		public long getValue() { return (m_value); }
		public SortedMap<String, String> getTags() { return m_tags; }
	}

	private static class CurrentTags extends ThreadLocal<SortedMap<String, String>>
	{
		@Override
		protected synchronized SortedMap<String, String> initialValue()
		{
			return new TreeMap<String, String>();
		}
	}

	private static class ReporterData extends ThreadLocal<LinkedList<ReporterDataPoint>>
	{
		@Override
		protected synchronized LinkedList<ReporterDataPoint> initialValue()
		{
			return (new LinkedList<ReporterDataPoint>());
		}

		public void addDataPoint(ReporterDataPoint dps)
		{
			get().addLast(dps);
		}

		public ReporterDataPoint getNextDataPoint()
		{
			return get().removeFirst();
		}

		public int getListSize()
		{
			return get().size();
		}
	}

	private static class ReporterTime extends ThreadLocal<Long>
	{
		@Override
		protected synchronized Long initialValue()
		{
			return (0L);
		}
	}

	private static ReporterData s_reporterData = new ReporterData();
	private static CurrentTags s_currentTags = new CurrentTags();
	private static ReporterTime s_reportTime = new ReporterTime();

	private ThreadReporter()
	{
	}

	public static void setReportTime(long time)
	{
		s_reportTime.set(time);
	}

	public static long getReportTime()
	{
		return s_reportTime.get();
	}

	public static void addTag(String name, String value)
	{
		s_currentTags.get().put(name, value);
	}

	public static void removeTag(String name)
	{
		s_currentTags.get().remove(name);
	}

	public static void clearTags()
	{
		s_currentTags.get().clear();
	}

	public static void addDataPoint(String metric, long value)
	{
		s_reporterData.addDataPoint(new ReporterDataPoint(metric, s_currentTags.get(), value));
	}

	public static void submitData(LongDataPointFactory dataPointFactory, KairosDatastore datastore) throws DatastoreException
	{
		while (s_reporterData.getListSize() != 0)
		{
			ReporterDataPoint dp = s_reporterData.getNextDataPoint();

			datastore.putDataPoint(dp.getMetricName(), dp.getTags(),					dataPointFactory.createDataPoint(s_reportTime.get(), dp.getValue()));
		}
	}

	/**
	 Used in finally block to clear out unsent data in case an exception occurred.
	 */
	public static void clear()
	{
		while (s_reporterData.getListSize() != 0)
			s_reporterData.getNextDataPoint();
	}

}
