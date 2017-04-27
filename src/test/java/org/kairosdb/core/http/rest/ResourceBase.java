package org.kairosdb.core.http.rest;


import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.kairosdb.core.GuiceKairosDataPointFactory;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.aggregator.AggregatorFactory;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPoint;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.QueryPluginFactory;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.core.groupby.GroupByFactory;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.WebServer;
import org.kairosdb.core.http.WebServletModule;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;
import org.kairosdb.testing.Client;
import org.kairosdb.util.SimpleStatsReporter;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

public abstract class ResourceBase
{
    private static final EventBus eventBus = new EventBus();
    private static WebServer server;

    static QueryQueuingManager queuingManager;
    static Client client;
    static TestDatastore datastore;

    @BeforeClass
    public static void startup() throws Exception
    {
        //This sends jersey java util logging to logback
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        datastore = new TestDatastore();
        queuingManager = new QueryQueuingManager(3, "localhost");

        Injector injector = Guice.createInjector(new WebServletModule(new Properties()), new AbstractModule()
        {
            @Override
            protected void configure()
            {
                bind(EventBus.class).toInstance(eventBus);
                //Need to register an exception handler
                bindListener(Matchers.any(), new TypeListener()
                {
                    public <I> void hear(TypeLiteral<I> typeLiteral, TypeEncounter<I> typeEncounter)
                    {
                        typeEncounter.register(new InjectionListener<I>()
                        {
                            public void afterInjection(I i)
                            {
                                eventBus.register(i);
                            }
                        });
                    }
                });
                bind(String.class).annotatedWith(Names.named(WebServer.JETTY_ADDRESS_PROPERTY)).toInstance("0.0.0.0");
                bind(Integer.class).annotatedWith(Names.named(WebServer.JETTY_PORT_PROPERTY)).toInstance(9001);
                bind(String.class).annotatedWith(Names.named(WebServer.JETTY_WEB_ROOT_PROPERTY)).toInstance("bogus");
                bind(Datastore.class).toInstance(datastore);
                bind(KairosDatastore.class).in(Singleton.class);
                bind(AggregatorFactory.class).to(TestAggregatorFactory.class);
                bind(GroupByFactory.class).to(TestGroupByFactory.class);
                bind(QueryParser.class).in(Singleton.class);
                bind(QueryQueuingManager.class).toInstance(queuingManager);
                bindConstant().annotatedWith(Names.named("HOSTNAME")).to("HOST");
                bindConstant().annotatedWith(Names.named("kairosdb.datastore.concurrentQueryThreads")).to(1);
                bindConstant().annotatedWith(Names.named("kairosdb.query_cache.keep_cache_files")).to(false);
                bind(KairosDataPointFactory.class).to(GuiceKairosDataPointFactory.class);
                bind(QueryPluginFactory.class).to(TestQueryPluginFactory.class);
                bind(SimpleStatsReporter.class);

                Properties props = new Properties();
                InputStream is = getClass().getClassLoader().getResourceAsStream("kairosdb.properties");
                try
                {
                    props.load(is);
                    is.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }

                //Names.bindProperties(binder(), props);
                bind(Properties.class).toInstance(props);

                bind(DoubleDataPointFactory.class)
                        .to(DoubleDataPointFactoryImpl.class).in(Singleton.class);
                bind(DoubleDataPointFactoryImpl.class).in(Singleton.class);

                bind(LongDataPointFactory.class)
                        .to(LongDataPointFactoryImpl.class).in(Singleton.class);
                bind(LongDataPointFactoryImpl.class).in(Singleton.class);

                bind(LegacyDataPointFactory.class).in(Singleton.class);
                bind(StringDataPointFactory.class).in(Singleton.class);

            }
        });
        server = injector.getInstance(WebServer.class);
        server.start();

        client = new Client();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        if (server != null)
        {
            server.stop();
        }
    }

    public static class TestDatastore implements Datastore
    {
        private DatastoreException m_toThrow = null;
        private Map<String, String> metadata = new HashMap<>();

        TestDatastore() throws DatastoreException
        {
        }

        void throwException(DatastoreException toThrow)
        {
            m_toThrow = toThrow;
        }

        @Override
        public void close() throws InterruptedException
        {
        }

        @Override
        public Iterable<String> getMetricNames()
        {
            return Arrays.asList("cpu", "memory", "disk", "network");
        }

        @Override
        public Iterable<String> getTagNames()
        {
            return Arrays.asList("server1", "server2", "server3");
        }

        @Override
        public Iterable<String> getTagValues()
        {
            return Arrays.asList("larry", "moe", "curly");
        }

        @Override
        public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException
        {
            if (m_toThrow != null)
                throw m_toThrow;

            try
            {
                Map<String, String> tags = new TreeMap<>();
                tags.put("server", "server1");

                queryCallback.startDataPointSet(LongDataPointFactoryImpl.DST_LONG, tags);
                queryCallback.addDataPoint(new LongDataPoint(1, 10));
                queryCallback.addDataPoint(new LongDataPoint(1, 20));
                queryCallback.addDataPoint(new LongDataPoint(2, 10));
                queryCallback.addDataPoint(new LongDataPoint(2, 5));
                queryCallback.addDataPoint(new LongDataPoint(3, 10));

                tags = new TreeMap<>();
                tags.put("server", "server2");

                queryCallback.startDataPointSet(DoubleDataPointFactoryImpl.DST_DOUBLE, tags);
                queryCallback.addDataPoint(new DoubleDataPoint(1, 10.1));
                queryCallback.addDataPoint(new DoubleDataPoint(1, 20.1));
                queryCallback.addDataPoint(new DoubleDataPoint(2, 10.1));
                queryCallback.addDataPoint(new DoubleDataPoint(2, 5.1));
                queryCallback.addDataPoint(new DoubleDataPoint(3, 10.1));

                queryCallback.endDataPoints();
            }
            catch (IOException e)
            {
                throw new DatastoreException(e);
            }
        }

        @Override
        public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException
        {
        }

        @Override
        public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException
        {
            return null;
        }

        @Override
        public void setValue(String service, String serviceKey, String key, String value) throws DatastoreException
        {
            if (m_toThrow != null)
                throw m_toThrow;
            metadata.put(service + "/" + serviceKey + "/" + key, value);
        }

        @Override
        public String getValue(String service, String serviceKey, String key) throws DatastoreException
        {
            if (m_toThrow != null)
                throw m_toThrow;
            return metadata.get(service + "/" + serviceKey + "/" + key);
        }

        @Override
        public Iterable<String> listServiceKeys(String service)
                throws DatastoreException
        {
            if (m_toThrow != null)
                throw m_toThrow;

            Set<String> keys = new HashSet<>();
            for (String key : metadata.keySet()) {
                if (key.startsWith(service))
                {
                    keys.add(key.split("/")[1]);
                }
            }
            return keys;
        }

        @Override
        public Iterable<String> listKeys(String service, String serviceKey) throws DatastoreException
        {
            if (m_toThrow != null)
                throw m_toThrow;

            List<String> keys = new ArrayList<>();
            for (String key : metadata.keySet()) {
                if (key.startsWith(service + "/" + serviceKey))
                {
                    keys.add(key.split("/")[2]);
                }
            }
            return keys;
        }

        @Override
        public Iterable<String> listKeys(String service, String serviceKey, String keyStartsWith) throws DatastoreException
        {
            if (m_toThrow != null)
                throw m_toThrow;

            List<String> keys = new ArrayList<>();
            for (String key : metadata.keySet()) {
                if (key.startsWith(service + "/" + serviceKey +  "/" + keyStartsWith))
                {
                    keys.add(key.split("/")[2]);
                }
            }
            return keys;
        }

        @Override
        public void deleteKey(String service, String serviceKey, String key)
                throws DatastoreException
        {
            if (m_toThrow != null)
                throw m_toThrow;

            metadata.remove(service + "/" + serviceKey + "/" + key);
        }
    }
}
