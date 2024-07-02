package io.stargate.sgv2.jsonapi.service.embedding;

import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentialResolver;
import io.stargate.sgv2.jsonapi.api.request.HeaderBasedEmbeddingCredentialResolver;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Produces;

/**
 * Simple CDI producer for the {@link EmbeddingCredentialResolver} to be used in the embedding
 * service
 */
@Singleton
public class EmbeddingApiKeyResolverProvider {
  @Inject HttpConstants httpConstants;

  @Produces
  @ApplicationScoped
  EmbeddingCredentialResolver headerTokenResolver() {
    return new HeaderBasedEmbeddingCredentialResolver(
        httpConstants.embeddingApiKey(),
        httpConstants.embeddingAccessId(),
        httpConstants.embeddingSecretId());
  }
}
