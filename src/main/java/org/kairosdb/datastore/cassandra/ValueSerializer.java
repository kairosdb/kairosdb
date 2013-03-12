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

package org.kairosdb.datastore.cassandra;

import java.nio.ByteBuffer;

public class ValueSerializer
{
	public static final byte FLOAT_VALUE = 0x1;
	public static final byte DOUBLE_VALUE = 0x2;

	public static ByteBuffer toByteBuffer(long value)
	{
		ByteBuffer buffer = ByteBuffer.allocate(8);
		boolean writeRest = false;

		if (value != 0L)  //Short circuit for zero values
		{
			for (int I = 1; I <= 8; I++)
			{
				byte b = (byte)((value >>> (64 - (8 * I))) & 0xFF);
				if (writeRest || b != 0)
				{
					buffer.put(b);
					writeRest = true;
				}
			}
		}

		buffer.flip();

		return (buffer);
	}

	public static long getLongFromByteBuffer(ByteBuffer byteBuffer)
	{
		long ret = 0L;

		while (byteBuffer.hasRemaining())
		{
			ret <<= 8;
			byte b = byteBuffer.get();
			ret |= (b & 0xFF);
		}

		return (ret);
	}


	public static ByteBuffer toByteBuffer(float value)
	{
		ByteBuffer buffer = ByteBuffer.allocate(5);

		buffer.put(FLOAT_VALUE);
		buffer.putFloat(value);

		buffer.flip();

		return (buffer);
	}


	public static double getDoubleFromByteBuffer(ByteBuffer byteBuffer)
	{
		byte flag = byteBuffer.get();
		double ret = 0;

		if (flag == FLOAT_VALUE)
			ret = byteBuffer.getFloat();
		else
			ret = byteBuffer.getDouble();

		return (ret);
	}
}
