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

import com.google.gson.JsonElement;
import org.kairosdb.core.DataPoint;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.kairosdb.core.DataPoint.GROUP_NUMBER;

public class NullDataPointFactory implements DataPointFactory
{

	public static final String DATASTORE_TYPE = "null";
	public static final String API_TYPE = "null";


	public static void writeToByteBuffer(DataOutput buffer, NullDataPoint dataPoint) throws IOException
	{
		buffer.writeByte(0x0);
	}

	@Override
	public String getDataStoreType()
	{
		return DATASTORE_TYPE;
	}

	@Override
	public String getGroupType()
	{
		return GROUP_NUMBER;
	}

	@Override
	public DataPoint getDataPoint(long timestamp, JsonElement json)
	{
		return new NullDataPoint(timestamp);
	}

	@Override
	public DataPoint getDataPoint(long timestamp, DataInput buffer) throws IOException
	{
		return new NullDataPoint(timestamp);
	}
}
