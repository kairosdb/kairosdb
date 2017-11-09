package org.kairosdb.datastore.cassandra.cache;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.kairosdb.datastore.cassandra.cache.persistence.GeneralHashCacheStore;

public class DefaultTagNameCache extends AbstractStringCache implements StringKeyCache {
    public static final String TAG_NAME_CACHE = "tagNameCache";

    @Inject
    public DefaultTagNameCache(@Named(TAG_NAME_CACHE) final GeneralHashCacheStore cacheStore,
                               final TagNameCacheConfiguration config) {
        super(cacheStore, config);
    }
}
