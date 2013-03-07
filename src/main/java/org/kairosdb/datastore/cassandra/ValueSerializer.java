// KairosDB2
// Copyright (C) 2013 Proofpoint, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>

package org.kairosdb.datastore.cassandra;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

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
