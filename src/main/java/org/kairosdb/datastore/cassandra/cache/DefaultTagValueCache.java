package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

public class DefaultTagValueCache extends AbstractStringCache implements StringKeyCache {
    public static final String TAG_VALUE_CACHE = "tagValueCache";

    @Inject
    public DefaultTagValueCache(@Named(TAG_VALUE_CACHE) final GeneralHashCacheStore cacheStore,
                                final TagValueCacheConfiguration config) {
        super(cacheStore, config);
    }
}
