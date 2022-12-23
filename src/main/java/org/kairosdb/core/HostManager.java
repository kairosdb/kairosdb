package org.kairosdb.core;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.exception.KairosDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
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

    private final ServiceKeyStore keyStore;
    private final String hostname;
    private final long inactiveTimeSeconds;
    private final String guid;
    private ScheduledExecutorService executorService;

    private volatile Map<String, ServiceKeyValue> activeHosts = new HashMap<>();
    private volatile boolean hostListChanged;

    @Inject
    public HostManager(ServiceKeyStore keyStore,
            @Named(HOST_MANAGER_SERVICE_EXECUTOR) ScheduledExecutorService executorService,
            @Named(DELAY) long delay, @Named("HOSTNAME") String hostName, @Named(INACTIVE_TIME) long inactiveTime,
            @Named(Main.KAIROSDB_SERVER_GUID) String guid)
    {
        this.keyStore = requireNonNull(keyStore, "keyStore cannot be null");
        this.executorService = requireNonNull(executorService, "executorService cannot be null");
        this.hostname = requireNonNullOrEmpty(hostName, "hostname cannot be null or empty");
        this.inactiveTimeSeconds = inactiveTime;
        this.guid = requireNonNullOrEmpty(guid, "guid cannot be null or empty");

        executorService.scheduleWithFixedDelay(new CheckChanges(), 0, delay, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    private class CheckChanges implements Runnable
    {
        @Override
        public void run()
        {
            checkHostChanges();
        }
    }

    @VisibleForTesting
    void checkHostChanges()
    {
        try {
            // Add this host to the table if it doesn't exist or update its timestamp
            keyStore.setValue(SERVICE, guid, hostname, "active");
            keyStore.setValue(SERVICE, SERVICE_KEY, guid, hostname);

            Map<String, ServiceKeyValue> hosts = getHostsFromKeyStore();

            // Remove inactive nodes from the table
            long now = System.currentTimeMillis();
            for (String guid : hosts.keySet()) {
                ServiceKeyValue host = hosts.get(guid);
                if ((host.getLastModified().getTime() + (1000 * inactiveTimeSeconds)) < now) {
                    keyStore.deleteKey(SERVICE, SERVICE_KEY, guid);
                }
            }

            hosts = getHostsFromKeyStore();
            if (!activeHosts.equals(hosts)) {
                logger.debug("Hosts list changed");
                hostListChanged = true;
            }

            // update cache
            activeHosts = hosts;
        }
        catch (Throwable e) {
            logger.error("Could not access keystore " + SERVICE + ":" + SERVICE_KEY);
        }
    }

    private Map<String, ServiceKeyValue> getHostsFromKeyStore()
            throws DatastoreException
    {
        Map<String, ServiceKeyValue> hosts = new HashMap<>();
        Iterable<String> guids = keyStore.listKeys(SERVICE, SERVICE_KEY);
        for (String guid : guids) {
            ServiceKeyValue value = keyStore.getValue(SERVICE, SERVICE_KEY, guid);
            if (value != null) {
                hosts.put(guid, value);
            }
        }
        return hosts;
    }

    /**
     * Returns a map of kairos hosts. The key is a guid and the value is the hostname. There should always be at least one in the map (the current kairos node).
     * @return list of kairos hosts.
     */
    public Map<String, ServiceKeyValue> getActiveKairosHosts()
    {
        return ImmutableMap.copyOf(activeHosts);
    }

    /**
     * Returns true if the host list has changed since last called and resets the value to false.
     */
    public boolean acknowledgeHostListChanged()
    {
        boolean changed = hostListChanged;
        hostListChanged = false;
        return changed;
    }

    @Override
    public void start()
            throws KairosDBException
    {
    }

    @Override
    public void stop()
    {
        executorService.shutdown();
    }
}
