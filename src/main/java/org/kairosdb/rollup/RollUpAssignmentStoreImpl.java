package org.kairosdb.rollup;

import com.google.inject.Inject;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class RollUpAssignmentStoreImpl implements RollUpAssignmentStore
{
    public static final Logger logger = LoggerFactory.getLogger(RollUpAssignmentStoreImpl.class);

    static final String SERVICE = "_Rollups";
    static final String SERVICE_KEY_ASSIGNMENTS = "Assignment";

    private final ServiceKeyStore serviceKeyStore;

    @Inject
    public RollUpAssignmentStoreImpl(ServiceKeyStore serviceKeyStore)
    {
        this.serviceKeyStore = checkNotNull(serviceKeyStore, "serviceKeyStore cannot be null");
    }

    @Override
    public long getLastModifiedTime()
            throws RollUpException
    {
        try {
            Date lastModifiedTime = serviceKeyStore.getServiceKeyLastModifiedTime(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            if (lastModifiedTime != null) {
                return lastModifiedTime.getTime();
            }
            return 0L;
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public Set<String> getAssignmentIds()
            throws RollUpException
    {
        try {
            Set<String> assignedIds = new HashSet<>();
            Iterable<String> keys = serviceKeyStore.listKeys(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            for (String key : keys) {
                assignedIds.add(key);
            }
            return assignedIds;
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public Map<String, String> getAssignments()
            throws RollUpException
    {
        try {
            Map<String, String> assignments = new HashMap<>();
            Iterable<String> keys = serviceKeyStore.listKeys(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            for (String key : keys) {
                assignments.put(key, serviceKeyStore.getValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, key).getValue());
            }
            return assignments;
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public Set<String> getAssignedIds(String host)
            throws RollUpException
    {
        Set<String> assignedTasks = new HashSet<>();
        try {
            Iterable<String> keys = serviceKeyStore.listKeys(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            for (String key : keys) {
                String assigned = serviceKeyStore.getValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, key).getValue();
                if (assigned.equals(host)) {
                    assignedTasks.add(key);
                }
            }
            return assignedTasks;
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public void setAssignment(String unassignedId, String hostName)
            throws RollUpException
    {
        checkNotNullOrEmpty(unassignedId, "unassignedId cannot be null or empty");
        checkNotNullOrEmpty(hostName, "hostName cannot be null or empty");

        try {
            serviceKeyStore.setValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, unassignedId, hostName);
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not write assignment to service keystore. Id: " + unassignedId + " value: " + hostName, e);
        }
    }

    @Override
    public void removeAssignments(Set<String> ids)
            throws RollUpException
    {
        try {
            for (String id : ids) {
                serviceKeyStore.deleteKey(SERVICE, SERVICE_KEY_ASSIGNMENTS, id);
            }
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not delete ids.", e);
        }
    }
}
