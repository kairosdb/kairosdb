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

import org.junit.Test;
import org.kairosdb.util.KDataInput;
import org.kairosdb.util.KDataOutput;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import static junit.framework.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.kairosdb.util.Util.packLong;
import static org.kairosdb.util.Util.unpackLong;

public class ValueSerializerTest
{

	@Test
	public void testLongs()
	{
		ByteBuffer buf = ValueSerializer.toByteBuffer(0L);
		assertThat(buf.remaining(), equalTo(0));
		assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(0L));

		buf = ValueSerializer.toByteBuffer(256);
		assertThat(buf.remaining(), equalTo(2));
		assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(256L));

		for (long I = 1; I < 0x100; I++)
		{
			buf = ValueSerializer.toByteBuffer(I);
			assertThat(buf.remaining(), equalTo(1));
			assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(I));
		}

		for (long I = 0x100; I < 0x10000; I++)
		{
			buf = ValueSerializer.toByteBuffer(I);
			assertThat(buf.remaining(), equalTo(2));
			assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(I));
		}

		for (long I = 0x10000; I < 0x1000000; I++)
		{
			buf = ValueSerializer.toByteBuffer(I);
			assertThat(buf.remaining(), equalTo(3));
			assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(I));
		}

		for (long I = 0x1000000; I < 0x100000000L; I += 0x400000)
		{
			buf = ValueSerializer.toByteBuffer(I);
			assertThat(buf.remaining(), equalTo(4));
			assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(I));
		}

		for (long I = 0x100000000L; I < 0x10000000000L; I += 0x40000000L)
		{
			buf = ValueSerializer.toByteBuffer(I);
			assertThat(buf.remaining(), equalTo(5));
			assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(I));
		}

		for (long I = 0x10000000000L; I < 0x1000000000000L; I += 0x4000000000L)
		{
			buf = ValueSerializer.toByteBuffer(I);
			assertThat(buf.remaining(), equalTo(6));
			assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(I));
		}

		for (long I = 0x1000000000000L; I < 0x100000000000000L; I += 0x400000000000L)
		{
			buf = ValueSerializer.toByteBuffer(I);
			assertThat(buf.remaining(), equalTo(7));
			assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(I));
		}

		for (long I = 0x100000000000000L; I < 0x7000000000000000L; I += 0x40000000000000L)
		{
			buf = ValueSerializer.toByteBuffer(I);
			assertThat(buf.remaining(), equalTo(8));
			assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(I));
		}

		buf = ValueSerializer.toByteBuffer(-1);
		assertThat(buf.remaining(), equalTo(8));
		assertThat(ValueSerializer.getLongFromByteBuffer(buf), equalTo(-1L));
	}

	private void testValue(long value) throws IOException
	{
		KDataOutput bufOutput = new KDataOutput();
		packLong(value, bufOutput);

		DataInput bufInput = KDataInput.createInput(bufOutput.getBytes());
		long resp = unpackLong(bufInput);

		assertThat(resp, equalTo(value));
	}

	@Test
	public void testPackUnpack() throws IOException
	{
		testValue(0L);

		testValue(256);

		for (long I = 1; I < 0x100; I++)
		{
			testValue(I);
		}

		for (long I = 0x100; I < 0x10000; I++)
		{
			testValue(I);
		}

		for (long I = 0x10000; I < 0x1000000; I++)
		{
			testValue(I);
		}

		for (long I = 0x1000000; I < 0x100000000L; I += 0x400000)
		{
			testValue(I);
		}

		for (long I = 0x100000000L; I < 0x10000000000L; I += 0x40000000L)
		{
			testValue(I);
		}

		for (long I = 0x10000000000L; I < 0x1000000000000L; I += 0x4000000000L)
		{
			testValue(I);
		}

		for (long I = 0x1000000000000L; I < 0x100000000000000L; I += 0x400000000000L)
		{
			testValue(I);
		}

		for (long I = 0x100000000000000L; I < 0x7000000000000000L; I += 0x40000000000000L)
		{
			testValue(I);
		}

		testValue(-1);
	}
}
