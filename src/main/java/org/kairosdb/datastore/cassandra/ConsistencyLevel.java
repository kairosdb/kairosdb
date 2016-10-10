/*
 * Copyright 2016 KairosDB Authors
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

package org.kairosdb.datastore.cassandra;

import me.prettyprint.hector.api.HConsistencyLevel;

/**
 Created with IntelliJ IDEA.
 User: bhawkins
 Date: 11/18/13
 Time: 8:42 AM
 To change this template use File | Settings | File Templates.
 */
public enum ConsistencyLevel
{
	ANY (HConsistencyLevel.ANY),
	ONE (HConsistencyLevel.ONE),
	TWO (HConsistencyLevel.TWO),
	THREE (HConsistencyLevel.THREE),
	LOCAL_QUORUM (HConsistencyLevel.LOCAL_QUORUM),
	EACH_QUORUM (HConsistencyLevel.EACH_QUORUM),
	QUORUM (HConsistencyLevel.QUORUM),
	ALL (HConsistencyLevel.ALL);

	private final HConsistencyLevel m_level;

	ConsistencyLevel(HConsistencyLevel level)
	{
		m_level = level;
	}

	public HConsistencyLevel getHectorLevel()
	{
		return (m_level);
	}
}
