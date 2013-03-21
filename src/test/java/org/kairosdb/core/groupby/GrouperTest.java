//
//  GrouperTest.java
//
// Copyright 2013, Proofpoint Inc. All rights reserved.
//        
package org.kairosdb.core.groupby;

import org.junit.Test;
import org.kairosdb.core.DataPoint;
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
		Grouper grouper = new Grouper();

		List<GroupBy> groupBys = new ArrayList<GroupBy>();
		groupBys.add(new ValueGroupBy(3));
		groupBys.add(new SimpleTimeGroupBy(2));

		ListDataPointGroup dataPointGroup1 = new ListDataPointGroup("dataPointGroup1");
		dataPointGroup1.addTag("host", "server1");
		dataPointGroup1.addTag("customer", "acme");
		dataPointGroup1.addDataPoint(new DataPoint(1, 0));
		dataPointGroup1.addDataPoint(new DataPoint(1, 1));
		dataPointGroup1.addDataPoint(new DataPoint(2, 2));
		dataPointGroup1.addDataPoint(new DataPoint(2, 3));
		dataPointGroup1.addDataPoint(new DataPoint(2, 4));

		ListDataPointGroup dataPointGroup2 = new ListDataPointGroup("dataPointGroup2");
		dataPointGroup2.addTag("host", "server2");
		dataPointGroup2.addTag("customer", "foobar");
		dataPointGroup2.addDataPoint(new DataPoint(2, 5));
		dataPointGroup2.addDataPoint(new DataPoint(2, 6));
		dataPointGroup2.addDataPoint(new DataPoint(2, 7));
		dataPointGroup2.addDataPoint(new DataPoint(2, 8));


		List<DataPointGroup> dataPointGroups = new ArrayList<DataPointGroup>();
		dataPointGroups.add(dataPointGroup1);
		dataPointGroups.add(dataPointGroup2);

		List<DataPointGroup> groups = grouper.group(groupBys, dataPointGroups);

		assertThat(groups.size(), equalTo(4));

		DataPointGroup group1 = groups.get(0);
		assertThat(group1.getTagValues("host"), hasItems("server1"));
		assertThat(group1.getTagValues("customer"), hasItems("acme"));

		assertThat(group1.next().getLongValue(), equalTo(0L));
		assertThat(group1.next().getLongValue(), equalTo(1L));
		assertThat(group1.next(), equalTo(null));
		group1.close();  // cleans up temp files

		DataPointGroup group2 = groups.get(1);
		assertThat(group2.getTagValues("host"), hasItems("server1"));
		assertThat(group2.getTagValues("customer"), hasItems("acme"));

		assertThat(group2.next().getLongValue(), equalTo(2L));
		assertThat(group2.next(), equalTo(null));
		group2.close();  // cleans up temp files

		DataPointGroup group3 = groups.get(2);
		assertThat(group3.getTagValues("host"), hasItems("server1"));
		assertThat(group3.getTagValues("customer"), hasItems("acme"));

		assertThat(group3.next().getLongValue(), equalTo(3L));
		assertThat(group3.next().getLongValue(), equalTo(4L));
		assertThat(group3.next().getLongValue(), equalTo(5L));
		assertThat(group3.next(), equalTo(null));
		group3.close();  // cleans up temp files

		DataPointGroup group4 = groups.get(3);
		assertThat(group4.getTagValues("host"), hasItems("server2"));
		assertThat(group4.getTagValues("customer"), hasItems("foobar"));

		assertThat(group4.next().getLongValue(), equalTo(6L));
		assertThat(group4.next().getLongValue(), equalTo(7L));
		assertThat(group4.next().getLongValue(), equalTo(8L));
		assertThat(group4.next(), equalTo(null));
		group4.close();  // cleans up temp files
	}

}