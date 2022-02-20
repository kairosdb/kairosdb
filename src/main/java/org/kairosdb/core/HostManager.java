package org.kairosdb.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;
import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;

public class HostManager implements KairosDBService
{
    private static final Logger logger = LoggerFactory.getLogger(HostManager.class);

    private static final String SERVICE = "_Hosts";
    private static final String SERVICE_KEY = "Active";
    private static final String DELAY = "kairosdb.host_service_manager.check_delay_time_millseconds";
    private static final String INACTIVE_TIME = "kairosdb.host_service_manager.inactive_time_seconds";
    static final String HOST_MANAGER_SERVICE_EXECUTOR = "HostManagerServiceExecutor";

    private final ServiceKeyStore m_keyStore;
    private final String m_hostname;
    private final long m_inactiveTimeSeconds;
    private final String m_guid;
    private final Publisher<CoordinatorChangeEvent> m_coordinatorPublisher;
    private final Publisher<HostChangeEvent> m_hostPublisher;
    private final long m_delay;
    private ScheduledExecutorService m_executorService;

    private volatile SortedMap<String, ServiceKeyValue> m_activeHosts = new TreeMap<>();
    private boolean m_isCoordinatorHost = false;

    @Inject
    public HostManager(ServiceKeyStore keyStore,
            @Named(HOST_MANAGER_SERVICE_EXECUTOR) ScheduledExecutorService executorService,
            @Named(DELAY) long delay, @Named("HOSTNAME") String hostName,
            @Named(INACTIVE_TIME) long inactiveTime,
            @Named(Main.KAIROSDB_SERVER_GUID) String guid, FilterEventBus eventBus)
    {
        m_keyStore = requireNonNull(keyStore, "keyStore cannot be null");
        m_executorService = requireNonNull(executorService, "executorService cannot be null");
        m_hostname = requireNonNullOrEmpty(hostName, "hostname cannot be null or empty");
        m_inactiveTimeSeconds = inactiveTime;
        m_guid = requireNonNullOrEmpty(guid, "guid cannot be null or empty");
        m_delay = delay;
        m_coordinatorPublisher = eventBus.createPublisher(CoordinatorChangeEvent.class);
        m_hostPublisher = eventBus.createPublisher(HostChangeEvent.class);


    }


    @VisibleForTesting
    void checkHostChanges()
    {
        try {
            logger.debug("Checking host list for changes");
            // Add this host to the table if it doesn't exist or update its timestamp
            m_keyStore.setValue(SERVICE, SERVICE_KEY, m_guid, m_hostname);

            SortedMap<String, ServiceKeyValue> hosts = getHostsFromKeyStore();

            // Remove inactive nodes from the table
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<String, ServiceKeyValue>> hostIterator = hosts.entrySet().iterator();
            while (hostIterator.hasNext())
            {
                Map.Entry<String, ServiceKeyValue> hostEntry = hostIterator.next();
                ServiceKeyValue host = hostEntry.getValue();
                if ((host.getLastModified().getTime() + (1000 * m_inactiveTimeSeconds)) < now)
                {
                    System.out.println("Expiring host: " + hostEntry.getKey());
                    logger.debug("Expiring host "+ hostEntry.getKey());
                    m_keyStore.deleteKey(SERVICE, SERVICE_KEY, hostEntry.getKey());
                    hostIterator.remove();
                }
            }

            if (!m_activeHosts.equals(hosts))
            {
                //Check if we are the controller host
                if (hosts.firstKey().equals(m_guid) && !m_isCoordinatorHost)
                {
                    logger.debug("We are the coordinator");
                    m_coordinatorPublisher.post(new CoordinatorChangeEvent(true));
                    m_isCoordinatorHost = true;
                }
                else if (m_isCoordinatorHost && !hosts.firstKey().equals(m_guid))
                {
                    logger.debug("No longer the coordinator.");
                    m_coordinatorPublisher.post(new CoordinatorChangeEvent(false));
                    m_isCoordinatorHost = false;
                }

                //This has to be after the coordinator change event
                logger.debug("Hosts list changed");
                m_hostPublisher.post(new HostChangeEvent(ImmutableMap.copyOf(hosts)));

                // update cache
                m_activeHosts = hosts;
            }


        }
        catch (Throwable e) {
            logger.error("Could not access keystore " + SERVICE + ":" + SERVICE_KEY);
        }
    }

    private SortedMap<String, ServiceKeyValue> getHostsFromKeyStore()
            throws DatastoreException
    {
        SortedMap<String, ServiceKeyValue> hosts = new TreeMap<>();
        Iterable<String> guids = m_keyStore.listKeys(SERVICE, SERVICE_KEY);
        for (String guid : guids) {
            ServiceKeyValue value = m_keyStore.getValue(SERVICE, SERVICE_KEY, guid);
            if (value != null) {
                hosts.put(guid, value);
            }
        }
        return hosts;
    }

    /**
     * Returns a map of kairos hosts. The key is a guid nd the value is the hostname. There should always be at least one in the map (the current kairos node).
     * @return list of kairos hosts.
     */
    public Map<String, ServiceKeyValue> getActiveKairosHosts()
    {
        return ImmutableMap.copyOf(m_activeHosts);
    }

    @Override
    public void start()
            throws KairosDBException
    {
        m_executorService.scheduleWithFixedDelay(this::checkHostChanges, 5000, m_delay, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop()
    {
        m_executorService.shutdown();
    }


    @ToString
    @EqualsAndHashCode
    public static class CoordinatorChangeEvent
    {
        private final boolean coordinator;

        /*package*/ CoordinatorChangeEvent(boolean coordinator)
        {
            this.coordinator = coordinator;
        }

        public boolean isCoordinator()
        {
            return coordinator;
        }
    }


    @ToString
    @EqualsAndHashCode
    public static class HostChangeEvent
    {
        private final Map<String, ServiceKeyValue> hostMap;

        /*package*/ HostChangeEvent(Map<String, ServiceKeyValue> hostMap)
        {
            this.hostMap = hostMap;
        }

        public Map<String, ServiceKeyValue> getHostMap()
        {
            return hostMap;
        }
    }
}
