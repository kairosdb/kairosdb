package org.kairosdb.datastore.cassandra.cache;

import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractByteBufferCacheTest {

    @Test
    public void testDoubleHashIs128bit() {
        final AbstractByteBufferCache cache = mock(AbstractByteBufferCache.class);
        when(cache.doubleHash(any(ByteBuffer.class))).thenCallRealMethod();
        final BigInteger got = cache.doubleHash(ByteBuffer.wrap(new byte[]{0x42, 0x69}));
        assertNotNull(got);
        assertThat(got.bitLength(), is(127)); // excludes the sign bit
    }
}