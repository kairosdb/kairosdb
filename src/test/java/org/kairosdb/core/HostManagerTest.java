package org.kairosdb.core;

import org.junit.Before;
import org.junit.Test;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.testing.FakeServiceKeyStore;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HostManagerTest
{
    private static final String SERVICE = "_Hosts";
    private static final String SERVICE_KEY = "Active";

    private HostManager manager;
    private FakeServiceKeyStore keyStore = new FakeServiceKeyStore();

    @Mock
    private ScheduledExecutorService mockExecutorService;

    @Mock
    private FilterEventBus eventBus;

    @Mock
    private Publisher<HostManager.HostChangeEvent> hostPublisher;

    @Mock
    private Publisher<HostManager.CoordinatorChangeEvent> coordinatorPublisher;

    @Before
    public void Setup()
    {
        initMocks(this);

        when(eventBus.createPublisher(HostManager.HostChangeEvent.class)).thenReturn(hostPublisher);
        when(eventBus.createPublisher(HostManager.CoordinatorChangeEvent.class)).thenReturn(coordinatorPublisher);
        manager = new HostManager(keyStore, mockExecutorService, 10, "myHost", 5, "myGuid", eventBus);
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
        assertThat(activeKairosHosts.size()).isEqualTo(4);
        assertThat(activeKairosHosts.get("1").getValue()).isEqualTo("host1");
        assertThat(activeKairosHosts.get("2").getValue()).isEqualTo("host2");
        assertThat(activeKairosHosts.get("3").getValue()).isEqualTo("host3");
        assertThat(activeKairosHosts.get("myGuid").getValue()).isEqualTo("myHost");

        assertThat(keyStore.getValue(SERVICE, SERVICE_KEY, "myGuid").getValue()).isEqualTo("myHost");
    }

    @Test
    public void test_checkHostChanges_expireInactive()
            throws DatastoreException
    {
        keyStore.setValue(SERVICE, SERVICE_KEY, "1", "host1");
        keyStore.setValue(SERVICE, SERVICE_KEY, "2", "host2");
        keyStore.setValue(SERVICE, SERVICE_KEY, "3", "host3");

        manager.checkHostChanges();

        long currentTime = System.currentTimeMillis();
        long timeChange = currentTime - (1000 * 10); // 10 seconds
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "1", new Date(timeChange));
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "3", new Date(timeChange));
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "myGuid", new Date(timeChange)); //This will get updated before next check

        manager.checkHostChanges();

        Map<String, ServiceKeyValue> activeKairosHosts = manager.getActiveKairosHosts();
        assertNull(activeKairosHosts.get("1"));
        assertThat(activeKairosHosts.get("2").getValue()).isEqualTo("host2");
        assertNull(activeKairosHosts.get("3"));
        assertThat(activeKairosHosts.get("myGuid").getValue()).isEqualTo("myHost"); // current host should always be there
    }

    @Test
    public void test_hostsChanged()
            throws DatastoreException
    {
        keyStore.setValue(SERVICE, SERVICE_KEY, "1", "host1");
        keyStore.setValue(SERVICE, SERVICE_KEY, "2", "host2");
        keyStore.setValue(SERVICE, SERVICE_KEY, "3", "host3");

        manager.checkHostChanges();

        long timeChange = 1000 * 10; // 10 seconds
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "1", new Date(timeChange));
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "3", new Date(timeChange));

        manager.checkHostChanges();

        timeChange = 1000 * 20; // 10 seconds
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "1", new Date(timeChange));
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "3", new Date(timeChange));

        manager.checkHostChanges();

        ArgumentCaptor<HostManager.HostChangeEvent> hostCaptor = ArgumentCaptor.forClass(HostManager.HostChangeEvent.class);
        verify(hostPublisher, times(2)).post(hostCaptor.capture());

        assertThat(hostCaptor.getAllValues().get(0).getHostMap().keySet()).containsOnly("1", "2", "3", "myGuid");
        assertThat(hostCaptor.getValue().getHostMap().keySet()).containsOnly("2", "myGuid");

        verifyNoMoreInteractions(hostPublisher);
    }

    @Test
    public void test_coordinatorChange() throws DatastoreException
    {
        HostManager localManager = new HostManager(keyStore, mockExecutorService, 10, "myHost", 5, "2", eventBus);

        keyStore.setValue(SERVICE, SERVICE_KEY, "1", "host1");
        keyStore.setValue(SERVICE, SERVICE_KEY, "3", "host3");

        localManager.checkHostChanges();

        long timeChange = 1000 * 10; // 10 seconds
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "1", new Date(timeChange));

        localManager.checkHostChanges();

        verify(coordinatorPublisher).post(new HostManager.CoordinatorChangeEvent(true));

        timeChange = System.currentTimeMillis();
        keyStore.setValue(SERVICE, SERVICE_KEY, "1", "host1");
        keyStore.setKeyModificationTime(SERVICE, SERVICE_KEY, "1", new Date(timeChange));

        localManager.checkHostChanges();

        verify(coordinatorPublisher).post(new HostManager.CoordinatorChangeEvent(false));

        verify(hostPublisher, times(3)).post(any());

        verifyNoMoreInteractions(hostPublisher);
        verifyNoMoreInteractions(coordinatorPublisher);
    }
}