package org.kairosdb.testing;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import org.kairosdb.core.datastore.ServiceKeyStore;
import org.kairosdb.core.datastore.ServiceKeyValue;
import org.kairosdb.core.exception.DatastoreException;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FakeServiceKeyStore implements ServiceKeyStore
{
    private Table<String, String, Map<String, ServiceKeyValue>> table = HashBasedTable.create();
    private Map<String, Date> timeStamps = new HashMap<>();

    @SuppressWarnings("Java8MapApi")
    @Override
    public void setValue(String service, String serviceKey, String key, String value)
            throws DatastoreException
    {
        Date now = new Date();
        Map<String, ServiceKeyValue> keyMap = getKeyMap(service, serviceKey);
        keyMap.put(key, new ServiceKeyValue(value, now));
        timeStamps.put(service + "_" + serviceKey, now);
    }

    @Override
    public ServiceKeyValue getValue(String service, String serviceKey, String key)
            throws DatastoreException
    {
        Map<String, ServiceKeyValue> keyMap = getKeyMap(service, serviceKey);
        return keyMap.get(key);
    }

    @Override
    public Iterable<String> listServiceKeys(String service)
            throws DatastoreException
    {
        Map<String, Map<String, Map<String, ServiceKeyValue>>> rowMap = table.rowMap();
        return rowMap.keySet();
    }

    @SuppressWarnings("Java8MapApi")
    @Override
    public Iterable<String> listKeys(String service, String serviceKey)
            throws DatastoreException
    {
        Map<String, ServiceKeyValue> map = table.get(service, serviceKey);
        if (map != null)
        {
            return map.keySet();
        }
        return ImmutableSet.of();
    }

    @Override
    public Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith)
            throws DatastoreException
    {
        Set<String> keys = getKeyMap(service, serviceKey).keySet();
        for (String key : keys) {
            if (key.startsWith(keyStartsWith)) {
                keys.add(key);
            }
        }
        return keys;
    }

    @Override
    public void deleteKey(String service, String serviceKey, String key)
            throws DatastoreException
    {
        Map<String, ServiceKeyValue> keyMap = getKeyMap(service, serviceKey);
        keyMap.remove(key);
        timeStamps.put(service + "_" + serviceKey, new Date());
    }

    @Override
    public Date getServiceKeyLastModifiedTime(String service, String serviceKey)
            throws DatastoreException
    {
        return timeStamps.get(service + "_" + serviceKey);
    }

    public void setKeyModificationTime(String service, String serviceKey, String key, Date lastModfiied)
    {
        Map<String, ServiceKeyValue> keyMap = getKeyMap(service, serviceKey);
        ServiceKeyValue value = keyMap.get(key);
        if (value != null) {
            keyMap.put(key, new ServiceKeyValue(value.getValue(), lastModfiied));
        }
    }

    private Map<String, ServiceKeyValue> getKeyMap(String service, String serviceKey)
    {
        Map<String, ServiceKeyValue> keyMap = table.get(service, serviceKey);
        if (keyMap == null) {
            keyMap = new HashMap<>();
            table.put(service, serviceKey, keyMap);
        }
        return keyMap;
    }

}
