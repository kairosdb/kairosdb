//
//  GrouperTest.java
//
// Copyright 2016, KairosDB Authors
//        
package org.kairosdb.core.groupby;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.TestDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.formatter.FormatterException;
import org.kairosdb.testing.ListDataPointGroup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;

public class GrouperTest
{

	@Test
	public void test() throws IOException, FormatterException
	{
		Grouper grouper = new Grouper(new TestDataPointFactory());

		List<GroupBy> groupBys = new ArrayList<GroupBy>();
		groupBys.add(new ValueGroupBy(3));
		groupBys.add(new SimpleTimeGroupBy(2));

		ListDataPointGroup dataPointGroup1 = new ListDataPointGroup("dataPointGroup1");
		dataPointGroup1.addTag("host", "server1");
		dataPointGroup1.addTag("customer", "acme");
		dataPointGroup1.addDataPoint(new LongDataPoint(1, 0));
		dataPointGroup1.addDataPoint(new LongDataPoint(1, 1));
		dataPointGroup1.addDataPoint(new LongDataPoint(2, 2));
		dataPointGroup1.addDataPoint(new LongDataPoint(2, 3));
		dataPointGroup1.addDataPoint(new LongDataPoint(2, 4));

		ListDataPointGroup dataPointGroup2 = new ListDataPointGroup("dataPointGroup2");
		dataPointGroup2.addTag("host", "server2");
		dataPointGroup2.addTag("customer", "foobar");
		dataPointGroup2.addDataPoint(new LongDataPoint(2, 5));
		dataPointGroup2.addDataPoint(new LongDataPoint(2, 6));
		dataPointGroup2.addDataPoint(new LongDataPoint(2, 7));
		dataPointGroup2.addDataPoint(new LongDataPoint(2, 8));


		List<DataPointGroup> dataPointGroups = new ArrayList<DataPointGroup>();
		dataPointGroups.add(dataPointGroup1);
		dataPointGroups.add(dataPointGroup2);

		List<DataPointGroup> groups = grouper.group(groupBys, dataPointGroups);

		assertThat(groups.size(), equalTo(5));

		// Group 1
		DataPointGroup group1 = groups.get(0);
		assertThat(group1.getTagValues("host"), hasItems("server1"));
		assertThat(group1.getTagValues("customer"), hasItems("acme"));

		assertDataPoint(group1.next(), 1, 0);
		assertDataPoint(group1.next(),1, 1);
		assertThat(group1.next(), equalTo(null));
		group1.close();  // cleans up temp files

		// Group 2
		DataPointGroup group2 = groups.get(1);
		assertThat(group2.getTagValues("host"), hasItems("server1"));
		assertThat(group2.getTagValues("customer"), hasItems("acme"));

		assertDataPoint(group2.next(), 2, 2);
		assertThat(group2.next(), equalTo(null));
		group2.close();  // cleans up temp files

		// Group 3
		DataPointGroup group3 = groups.get(2);
		assertThat(group3.getTagValues("host"), hasItems("server1"));
		assertThat(group3.getTagValues("customer"), hasItems("acme"));

		assertDataPoint(group3.next(), 2, 3);
		assertDataPoint(group3.next(), 2, 4);
		assertThat(group3.next(), equalTo(null));
		group3.close();  // cleans up temp files

		// Group 4
		DataPointGroup group4 = groups.get(3);
		assertThat(group4.getTagValues("host"), hasItems("server2"));
		assertThat(group4.getTagValues("customer"), hasItems("foobar"));

		assertDataPoint(group4.next(), 2, 5);
		assertThat(group4.next(), equalTo(null));
		group4.close();  // cleans up temp files

		// Group 5
		DataPointGroup group5 = groups.get(4);
		assertThat(group5.getTagValues("host"), hasItems("server2"));
		assertThat(group5.getTagValues("customer"), hasItems("foobar"));

		assertDataPoint(group5.next(), 2, 6);
		assertDataPoint(group5.next(), 2, 7);
		assertDataPoint(group5.next(), 2, 8);
		assertThat(group5.next(), equalTo(null));
		group5.close();  // cleans up temp files
	}

	public static void assertDataPoint(DataPoint dataPoint, long expectedTimestamp, long expectedValue)
	{
		assertThat(dataPoint.getTimestamp(), equalTo(expectedTimestamp));
		assertThat(dataPoint.getLongValue(), equalTo(expectedValue));
	}

}