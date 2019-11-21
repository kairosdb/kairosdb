package org.kairosdb.core.onlineconfig;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.stups.tokens.AccessTokens;

import java.io.IOException;
import java.util.Optional;

public class EntityResolver {
    public static final Logger logger = LoggerFactory.getLogger(EntityResolver.class);

    private final ObjectMapper objectMapper;
    private final Executor executor;
    private final AccessTokens accessTokens;
    private final String hostname;

    @Inject
    public EntityResolver(ObjectMapper objectMapper, Executor executor, AccessTokens accessTokens, @Named("zmon.hostname") String hostname) {
        this.objectMapper = objectMapper;
        this.executor = executor;
        this.accessTokens = accessTokens;
        this.hostname = hostname;
    }

    public Optional<JsonNode> getEntityData(final String entityId) {
        final String uri = hostname + "/api/v1/entities/" + entityId;
        final Request request = Request.Get(uri).addHeader("Authorization", "Bearer " + accessTokens.get("zmon-read"));
        final JsonNode jsonNode;
        try {
            final String response = executor.execute(request).returnContent().toString();
            jsonNode = objectMapper.readTree(response);
            return Optional.ofNullable(jsonNode.get("data"));
        } catch (IOException e) {
            logger.warn(String.format("Failed fetching the entity '%s'", entityId), e);
            return Optional.empty();
        }
    }

    public Optional<Integer> getIntValue(JsonNode dataNode, String key) {
        if (dataNode == null) {
            logger.debug("No data node provided");
            return Optional.empty();
        }
        JsonNode intNode = dataNode.get(key);
        if (intNode == null) {
            logger.debug(String.format("There is no node for key '%s'", key));
            return Optional.empty();
        }
        return Optional.of(intNode.asInt());
    }

    public Optional<Boolean> getBooleanValue(JsonNode dataNode, String key) {
        if (dataNode == null) {
            logger.debug("No data node provided");
            return Optional.empty();
        }
        JsonNode intNode = dataNode.get(key);
        if (intNode == null) {
            logger.debug(String.format("There is no node for key '%s'", key));
            return Optional.empty();
        }
        return Optional.of(intNode.asBoolean());
    }
}
