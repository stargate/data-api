package io.stargate.sgv2.jsonapi;

import io.stargate.sgv2.jsonapi.api.request.EmbeddingApiKeyResolver;
import io.stargate.sgv2.jsonapi.api.request.HeaderBasedEmbeddingApiKeyResolver;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Produces;

@Singleton
public class EmbeddingApiKeyResolverProvider {
  @Inject HttpConstants httpConstants;

  @Produces
  @ApplicationScoped
  EmbeddingApiKeyResolver headerTokenResolver() {
    return new HeaderBasedEmbeddingApiKeyResolver(httpConstants.embeddingApiKey());
  }
}
