package org.kairosdb.core.tiers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.fluent.Executor;
import org.apache.http.impl.client.HttpClientBuilder;

public class ExecutorProvider implements Provider<Executor> {
    private final int timeout;

    @Inject
    public ExecutorProvider(@Named("kairosdb.datastore.datapoints.read.timeout") int timeout) {
        this.timeout = timeout;
    }

    @Override
    public Executor get() {
        final RequestConfig config = RequestConfig.custom()
                .setSocketTimeout(timeout)
                .setConnectTimeout(timeout)
                .build();
        HttpClient httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(config)
                .build();

        return Executor.newInstance(httpClient);
    }
}
