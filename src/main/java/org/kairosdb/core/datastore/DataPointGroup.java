/*
 * Copyright 2013 Proofpoint Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.datastore;

import org.kairosdb.core.DataPoint;

import java.util.Iterator;
import java.util.Set;

public interface DataPointGroup extends Iterator<DataPoint>
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
	 Returns the tag values for the given tag name.  After a grouping or aggregation
	 occurs a tag could have multiple values.
	 @param tag Tag to get the value for
	 @return A set of tag values
	 */
	public Set<String> getTagValues(String tag);

	/**
	 Close any underlying resources held open by this DataPointGroup.  This
	 will be called at the end of a query to free up resources.
	 */
	public void close();


}
