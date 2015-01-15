/*
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
package org.kairosdb.core.datapoints;

import org.json.JSONException;
import org.json.JSONWriter;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datapoints.DataPointHelper;
import org.kairosdb.core.datapoints.LongDataPoint;

import java.nio.ByteBuffer;

import org.kairosdb.core.aggregator.DataGapsMarkingAggregator;
import org.kairosdb.core.exception.KairosDBException;

import java.io.DataOutput;
import java.io.IOException;


public class NullDataPoint extends DataPointHelper
{
	public static final String API_TYPE = "null";

	public NullDataPoint(long timestamp)
	{
		super(timestamp);
	}


	@Override
	public String getDataStoreDataType()
	{
		return "null";
	}

	@Override
	public void writeValueToBuffer(DataOutput buffer) throws IOException
	{
		// write nothing - only used for query results
	}

	@Override
	public void writeValueToJson(JSONWriter writer) throws JSONException
	{
		writer.value(null);
	}

	@Override
	public boolean isLong()
	{
		return false;
	}

	@Override
	public long getLongValue()
	{
		throw new IllegalArgumentException("No aggregator can be chained after " + DataGapsMarkingAggregator.class.getName());
	}

	@Override
	public boolean isDouble()
	{
		return false;
	}

	@Override
	public double getDoubleValue()
	{
		throw new IllegalArgumentException("No aggregator can be chained after " + DataGapsMarkingAggregator.class.getName());
	}

	@Override
	public String getApiDataType()
	{
		return API_TYPE;
	}
}
