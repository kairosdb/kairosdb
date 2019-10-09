package org.kairosdb.core.tiers;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import org.zalando.stups.tokens.AccessTokens;
import org.zalando.stups.tokens.Tokens;

import java.net.URI;

public class AccessTokensProvider implements Provider<AccessTokens> {

    private final String tokensUri;

    @Inject
    public AccessTokensProvider(@Named("tokens.uri") String tokensUri) {
        this.tokensUri = tokensUri;
    }

    @Override
    public AccessTokens get() {
        return Tokens.createAccessTokensWithUri(URI.create(tokensUri))
                .manageToken("zmon-read").addScope("uid")
                .done().start();
    }
}
