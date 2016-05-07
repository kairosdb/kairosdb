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
import org.kairosdb.core.groupby.GroupByResult;

import java.util.Iterator;
import java.util.List;

public interface DataPointGroup extends Iterator<DataPoint>, TagSet
{
	/**
	 Returns the metric name for this group
	 @return Metric name
	 */
	public String getName();

	/**
	 * Returns the list of group by results or an empty list if the results are not grouped.
	 *
	 * @return list of group by results
	 */
	public List<GroupByResult> getGroupByResult();

	/**
	 Returns the api data type for this group
	 @return
	 */
	/*public String getAPIDataType();*/

	/**
	 Close any underlying resources held open by this DataPointGroup.  This
	 will be called at the end of a query to free up resources.
	 */
	public void close();


}
