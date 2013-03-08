//
// DataPointRow.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;

import java.util.Iterator;
import java.util.Set;

public interface DataPointRow extends Iterator<DataPoint>
{
	/**
	 Returns the metric name for this group
	 @return GSMetric name
	 */
	public String getName();

	/**
	 Returns a set of tag names associated with this group of data points
	 @return Set of tag names
	 */
	public Set<String> getTagNames();

	/**
	 Returns the tag value for the given tag name.
	 @param tag Tag to get the value for
	 @return A tag value
	 */
	public String getTagValue(String tag);

	/**
	 Close any underlying resources held open by this DataPointGroup.  This
	 will be called at the end of a query to free up resources.
	 */
	public void close();
}
