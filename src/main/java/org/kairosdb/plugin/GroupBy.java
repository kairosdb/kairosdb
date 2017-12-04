/*
 * Copyright 2016 KairosDB Authors
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
package org.kairosdb.plugin;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.groupby.GroupByResult;

import java.util.Map;

public interface GroupBy
{
	int getGroupId(DataPoint dataPoint, Map<String, String> tags);

	GroupByResult getGroupByResult(int id);

	/**
	 * Called when the object is instantiated with the query start date.
	 *
	 * @param startDate query start date
	 */
	void setStartDate(long startDate);
}