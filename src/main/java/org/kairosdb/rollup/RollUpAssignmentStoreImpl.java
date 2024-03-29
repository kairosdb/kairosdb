package org.kairosdb.rollup;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.util.Objects.requireNonNull;
import static org.kairosdb.util.Preconditions.requireNonNullOrEmpty;

public class RollUpAssignmentStoreImpl implements RollUpAssignmentStore
{
    public static final Logger logger = LoggerFactory.getLogger(RollUpAssignmentStoreImpl.class);

    static final String SERVICE = "_Rollups";
    static final String SERVICE_KEY_ASSIGNMENTS = "Assignment";

    private final ServiceKeyStore serviceKeyStore;

    @Inject
    public RollUpAssignmentStoreImpl(ServiceKeyStore serviceKeyStore)
    {
        this.serviceKeyStore = requireNonNull(serviceKeyStore, "serviceKeyStore cannot be null");
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
                ServiceKeyValue value = serviceKeyStore.getValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, key);
                if (value == null){
                    // something bad happened. This should have not exist without a value
                    logger.error("Assignment has a key but no value removing entry for key " + key);
                    removeAssignments(ImmutableSet.of(key));
                }
                else
                {
                    assignments.put(key, value.getValue());
                }
            }
            return assignments;
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not read from service keystore", e);
        }
    }

    @Override
    public Set<String> getAssignedIds(String hostId)
            throws RollUpException
    {
        Set<String> assignedTasks = new HashSet<>();
        try {
            Iterable<String> keys = serviceKeyStore.listKeys(SERVICE, SERVICE_KEY_ASSIGNMENTS);
            for (String key : keys) {
                String assigned = serviceKeyStore.getValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, key).getValue();
                if (assigned.equals(hostId)) {
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
        requireNonNullOrEmpty(unassignedId, "unassignedId cannot be null or empty");
        requireNonNullOrEmpty(hostName, "hostName cannot be null or empty");

        try {
            serviceKeyStore.setValue(SERVICE, SERVICE_KEY_ASSIGNMENTS, unassignedId, hostName);
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not write assignment to service keystore. Id: " + unassignedId + " value: " + hostName, e);
        }
    }

    public void removeAssignment(String taskId) throws RollUpException
    {
        try
        {
            serviceKeyStore.deleteKey(SERVICE, SERVICE_KEY_ASSIGNMENTS, taskId);
        }
        catch (DatastoreException e) {
            throw new RollUpException("Could not delete ids.", e);
        }
    }

    @Override
    public void removeAssignments(Set<String> ids)
            throws RollUpException
    {
        for (String id : ids) {
            removeAssignment(id);
        }
    }
}
