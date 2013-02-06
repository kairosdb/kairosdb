// OpenTSDB2
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

package net.opentsdb.datastore.cassandra;

import me.prettyprint.cassandra.serializers.AbstractSerializer;

import java.nio.ByteBuffer;

public class LongOrDoubleSerializer extends AbstractSerializer<LongOrDouble>
{
	public static final byte LONG_VALUE = 0x1;
	public static final byte DOUBLE_VALUE = 0x2;

	@Override
	public ByteBuffer toByteBuffer(LongOrDouble longOrDouble)
	{
		ByteBuffer buffer = ByteBuffer.allocate(9);

		if (longOrDouble.isLong())
		{
			buffer.put(LONG_VALUE);
			buffer.putLong(longOrDouble.getLongValue());
		}
		else
		{
			buffer.put(DOUBLE_VALUE);
			buffer.putDouble(longOrDouble.getDoubleValue());
		}

		buffer.flip();

		return (buffer);
	}

	@Override
	public LongOrDouble fromByteBuffer(ByteBuffer byteBuffer)
	{
		byte flag = byteBuffer.get();
		LongOrDouble ret = null;

		if (flag == LONG_VALUE)
			ret = new LongOrDouble(byteBuffer.getLong());
		else
			ret = new LongOrDouble(byteBuffer.getDouble());

		return (ret);
	}
}
