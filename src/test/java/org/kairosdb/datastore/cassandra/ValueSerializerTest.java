//
// ValueSerializerTest.java
//
// Copyright 2013, NextPage Inc. All rights reserved.
//

package org.kairosdb.datastore.cassandra;

import org.junit.Test;

import java.nio.ByteBuffer;

import static junit.framework.Assert.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
}
