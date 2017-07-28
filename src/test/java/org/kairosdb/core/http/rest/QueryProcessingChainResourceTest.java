package org.kairosdb.core.http.rest;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kairosdb.core.DataPointListener;
import org.kairosdb.core.DataPointListenerProvider;
import org.kairosdb.core.GuiceKairosDataPointFactory;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.KairosQueryProcessingChain;
import org.kairosdb.core.aggregator.Aggregator;
import org.kairosdb.core.aggregator.TestAggregatorFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactory;
import org.kairosdb.core.datapoints.DoubleDataPointFactoryImpl;
import org.kairosdb.core.datapoints.LegacyDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactory;
import org.kairosdb.core.datapoints.LongDataPointFactoryImpl;
import org.kairosdb.core.datapoints.StringDataPointFactory;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryPluginFactory;
import org.kairosdb.core.datastore.QueryQueuingManager;
import org.kairosdb.core.groupby.GroupBy;
import org.kairosdb.core.groupby.TestGroupByFactory;
import org.kairosdb.core.http.WebServer;
import org.kairosdb.core.http.WebServletModule;
import org.kairosdb.core.http.rest.json.QueryParser;
import org.kairosdb.core.http.rest.json.TestQueryPluginFactory;
import org.kairosdb.core.processingstage.QueryProcessingChain;
import org.kairosdb.core.processingstage.QueryProcessingStageFactory;
import org.kairosdb.testing.Client;
import org.kairosdb.testing.JsonResponse;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.kairosdb.core.http.rest.MetricsResourceTest.assertResponse;

public class QueryProcessingChainResourceTest
{
    private static final String QUERY_PROCESSING_STAGE_URL = "http://localhost:9001/api/v1/queryprocessing/stages/";
    private static final String QUERY_PROCESSING_CHAIN_URL = "http://localhost:9001/api/v1/queryprocessing/chain";

    private static MetricsResourceTest.TestDatastore datastore;
    private static QueryQueuingManager queuingManager;
    private static Client client;
    private static WebServer server;

    @BeforeClass
    public static void startup() throws Exception
    {
        //This sends jersey java util logging to logback
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        datastore = new MetricsResourceTest.TestDatastore();
        queuingManager = new QueryQueuingManager(3, "localhost");

        Injector injector = Guice.createInjector(new WebServletModule(new Properties()), new AbstractModule()
        {
            @Override
            protected void configure()
            {
                bind(String.class).annotatedWith(Names.named(WebServer.JETTY_ADDRESS_PROPERTY)).toInstance("0.0.0.0");
                bind(Integer.class).annotatedWith(Names.named(WebServer.JETTY_PORT_PROPERTY)).toInstance(9001);
                bind(String.class).annotatedWith(Names.named(WebServer.JETTY_WEB_ROOT_PROPERTY)).toInstance("bogus");
                bind(Datastore.class).toInstance(datastore);
                bind(KairosDatastore.class).in(Singleton.class);
                bind(QueryProcessingChain.class).to(KairosQueryProcessingChain.class).in(Singleton.class);
                bind(new TypeLiteral<QueryProcessingStageFactory<Aggregator>>() {}).to(TestAggregatorFactory.class);
                bind(new TypeLiteral<QueryProcessingStageFactory<GroupBy>>() {}).to(TestGroupByFactory.class);
                bind(QueryParser.class).in(Singleton.class);
                bind(new TypeLiteral<List<DataPointListener>>(){}).toProvider(DataPointListenerProvider.class);
                bind(QueryQueuingManager.class).toInstance(queuingManager);
                bindConstant().annotatedWith(Names.named("HOSTNAME")).to("HOST");
                bindConstant().annotatedWith(Names.named("kairosdb.datastore.concurrentQueryThreads")).to(1);
                bindConstant().annotatedWith(Names.named("kairosdb.query_cache.keep_cache_files")).to(false);
                bind(KairosDataPointFactory.class).to(GuiceKairosDataPointFactory.class);
                bind(QueryPluginFactory.class).to(TestQueryPluginFactory.class);

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
            server.stop();
    }

    @Test
    public void testGetAggregatorList() throws IOException
    {
        JsonResponse response = client.get(QUERY_PROCESSING_STAGE_URL + "aggregators");
        assertResponse(response, 200, "[]");
    }

    @Test
    public void testGetInvalidQueryProcessorList() throws IOException
    {
        JsonResponse response = client.get(QUERY_PROCESSING_STAGE_URL + "intel");
        assertResponse(response, 404, "{\"errors\":[\"Unknown processing stage family 'intel'\"]}");
    }

    @Test
    public void getTestGetQueryProcessingChain() throws IOException
    {
        JsonResponse response = client.get(QUERY_PROCESSING_CHAIN_URL);
        assertResponse(response, 200, "[{\"name\":\"group_by\",\"label\":\"Test GroupBy\",\"properties\":[]},{\"name\":\"aggregators\",\"label\":\"Test Aggregator\",\"properties\":[]}]");
    }
}
