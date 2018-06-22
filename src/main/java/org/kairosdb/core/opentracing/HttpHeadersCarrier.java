package org.kairosdb.core.opentracing;

import com.sun.jersey.core.util.MultivaluedMapImpl;
import io.opentracing.contrib.web.servlet.filter.HttpServletRequestExtractAdapter;
import io.opentracing.propagation.TextMap;

import javax.ws.rs.core.MultivaluedMap;

import java.util.Iterator;
import java.util.Map;

public class HttpHeadersCarrier implements TextMap {

    public HttpHeadersCarrier() {
        this.httpHeaders = new MultivaluedMapImpl();
    }

    private final MultivaluedMap<String, String> httpHeaders;

    public HttpHeadersCarrier(MultivaluedMap<String, String> httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    @Override
    public void put(String key, String value) {
        throw new UnsupportedOperationException(
                HttpHeadersCarrier.class.getName() + " should only be used with Tracer.extract()");
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator() {
        return new HttpServletRequestExtractAdapter.MultivaluedMapFlatIterator<>(httpHeaders.entrySet());
    }

}