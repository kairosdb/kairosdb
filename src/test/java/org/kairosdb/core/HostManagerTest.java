package org.kairosdb.core;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.testing.FakeServiceKeyStore;
import org.mockito.Mock;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.MockitoAnnotations.initMocks;

public class HostManagerTest
{
    private static final String SERVICE = "_Hosts";
    private static final String SERVICE_KEY = "Active";

    private HostManager manager;
    private FakeServiceKeyStore keyStore = new FakeServiceKeyStore();

    @Mock
    private ScheduledExecutorService mockExecutorService;

    @Before
    public void Setup()
    {
        initMocks(this);
        manager = new HostManager(keyStore, mockExecutorService, 10, "myHost", 5, "myGuid");
    }

    @Test
    public void test_checkHostChanges()
            throws DatastoreException
    {
        keyStore.setValue(SERVICE, SERVICE_KEY, "1", "host1");
        keyStore.setValue(SERVICE, SERVICE_KEY, "2", "host2");
        keyStore.setValue(SERVICE, SERVICE_KEY, "3", "host3");

        manager.checkHostChanges();

        Map<String, ServiceKeyValue> activeKairosHosts = manager.getActiveKairosHosts();
        assertThat(activeKairosHosts.size(), equalTo(4));
        assertThat(activeKairosHosts.get("1").getValue(), equalTo("host1"));
        assertThat(activeKairosHosts.get("2").getValue(), equalTo("host2"));
        assertThat(activeKairosHosts.get("3").getValue(), equalTo("host3"));
        assertThat(activeKairosHosts.get("myGuid").getValue(), equalTo("myHost"));

        assertThat(keyStore.getValue(SERVICE, SERVICE_KEY, "myGuid").getValue(), equalTo("myHost"));
    }

    @Test
    public void test_checkHostChanges_expireInactive()
            throws DatastoreException
    {
        keyStore.setValue(SERVICE, SERVICE_KEY, "1", "host1");
        keyStore.setValue(SERVICE, SERVICE_KEY, "2", "host2");
        keyStore.setValue(SERVICE, SERVICE_KEY, "3", "host3");

        manager.checkHostChanges();

        long timeChange = 1000 * 10; // 10 seconds
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "1", new Date(timeChange));
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "3", new Date(timeChange));
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "myGuid", new Date(timeChange));

        manager.checkHostChanges();

        Map<String, ServiceKeyValue> activeKairosHosts = manager.getActiveKairosHosts();
        assertNull(activeKairosHosts.get("1"));
        assertThat(activeKairosHosts.get("2").getValue(), equalTo("host2"));
        assertNull(activeKairosHosts.get("3"));
        assertThat(activeKairosHosts.get("myGuid").getValue(), equalTo("myHost")); // current host should always be there
    }
}