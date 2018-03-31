package org.kairosdb.rollup;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Date;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class RollupTaskStatusStoreImplTest extends RollupTestBase
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final String HOST1 = "host1";
    private static final String HOST2 = "host2";
    private static final String HOST3 = "host3";

    private static final Date DATE1 = new Date();
    private static final Date DATE2 = new Date();
    private static final Date DATE3 = new Date();

    private static final String ID1 = "ID1";
    private static final String ID2 = "ID2";
    private static final String ID3 = "ID3";

    private RollupTaskStatusStore store;

    @Before
    public void setup()
    {
        store = new RollupTaskStatusStoreImpl(fakeServiceKeyStore);
    }

    @Test
    public void test_writeRead_nullId_Invalid()
            throws RollUpException
    {
        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("id cannot be null or empty");

        store.write(null, new RollupTaskStatus(new Date(), "host"));
    }

    @Test
    public void test_writeRead_emptyId_Invalid()
            throws RollUpException
    {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("id cannot be null or empty");

        store.write("", new RollupTaskStatus(new Date(), "host"));
    }

    @Test
    public void test_writeReadRemove()
            throws RollUpException
    {
        RollupTaskStatus status1 = new RollupTaskStatus(DATE1, HOST1);
        RollupTaskStatus status2 = new RollupTaskStatus(DATE2, HOST2);
        RollupTaskStatus status3 = new RollupTaskStatus(DATE3, HOST3);

        store.write(ID1, status1);
        store.write(ID2, status2);
        store.write(ID3, status3);

        assertThat(store.read(ID1), equalTo(status1));
        assertThat(store.read(ID2), equalTo(status2));
        assertThat(store.read(ID3), equalTo(status3));

        store.remove(ID2);
        store.remove(ID1);

        assertNull(store.read(ID1));
        assertNull(store.read(ID2));
        assertThat(store.read(ID3), equalTo(status3));
    }
}