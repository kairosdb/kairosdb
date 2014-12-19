package org.kairosdb.util;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by irinaguberman on 12/18/14.
 */
public class InternalMetricsServerModule extends AbstractModule
    {
        public static final Logger logger = LoggerFactory.getLogger(InternalMetricsServerModule.class);

        private Properties m_props;

        public InternalMetricsServerModule(Properties props)
        {
            m_props = props;
        }

        @Override
        protected void configure()
        {
            logger.info("Configuring module InternalMetricsServerModule");

            bind(InternalMetricsServer.class).in(Singleton.class);
        }
    }
