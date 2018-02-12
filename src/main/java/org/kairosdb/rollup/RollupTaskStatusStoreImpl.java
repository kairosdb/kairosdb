package org.kairosdb.rollup;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Inject;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.util.Preconditions.checkNotNullOrEmpty;

public class RollupTaskStatusStoreImpl implements RollupTaskStatusStore
{
    public static final Logger logger = LoggerFactory.getLogger(RollupTaskStatusStoreImpl.class);

    static final String SERVICE = "_Rollups";
    static final String SERVICE_KEY_STATUS = "Status";

    private final ServiceKeyStore keyStore;
    private final Gson gson = new GsonBuilder().create();

    @Inject
    public RollupTaskStatusStoreImpl(ServiceKeyStore keyStore)
    {
        this.keyStore = checkNotNull(keyStore, "keystore cannot be null");
    }

    @Override
    public void write(String id, RollupTaskStatus status)
            throws RollUpException
    {
        try {
            checkNotNullOrEmpty(id, "id cannot be null or empty");
            checkNotNull(status, "status cannot be null or empty");
            String json = gson.toJson(status);
            keyStore.setValue(SERVICE, SERVICE_KEY_STATUS, id, json);
        }
        catch (DatastoreException e) {
            throw new RollUpException("Failed to write task status to the service keystore", e);
        }
    }

    @Override
    public RollupTaskStatus read(String id)
            throws RollUpException
    {
        try {
            checkNotNullOrEmpty(id, "id cannot be null or empty");
            ServiceKeyValue value = keyStore.getValue(SERVICE, SERVICE_KEY_STATUS, id);
            if (value != null)
            {
                return gson.fromJson(value.getValue(), RollupTaskStatus.class);
            }
        }
        catch (JsonSyntaxException e)
        {
            throw new RollUpException("Could not deserialize the status for id " + id, e);
        }
        catch (DatastoreException e) {
            throw new RollUpException("Failed to read the task status for id " + id, e);
        }
        return null;
    }

    @Override
    public void remove(String id)
            throws RollUpException
    {
        try {
            checkNotNullOrEmpty(id, "id cannot be null or empty");
            keyStore.deleteKey(SERVICE, SERVICE_KEY_STATUS, id);
        }
        catch (DatastoreException e) {
            throw new RollUpException("Failed to delete task status for id " + id, e);
        }
    }
}
