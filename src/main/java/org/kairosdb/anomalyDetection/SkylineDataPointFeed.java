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
package org.kairosdb.anomalyDetection;

import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointSet;

public class SkylineDataPointFeed implements DataPointListener
{
	private PicklePusher picklePusher;

	public SkylineDataPointFeed()
	{
		this.picklePusher = new PicklePusher();
	}

	@Override
	public void dataPoints(DataPointSet dataPointSet)
	{
		if (!dataPointSet.getName().startsWith("kairos"))
			picklePusher.pushDataPoints(dataPointSet);
	}
}