package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import java.util.Optional;

public class EmbeddingCredentialsSupplier {
  private final String tokenHeaderName;
  private final String embeddingApiKeyHeaderName;
  private final String embeddingAccessIdHeaderName;
  private final String embeddingsecretIdHeaderName;
  private EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig authConfig;

  public EmbeddingCredentialsSupplier(
      String tokenHeaderName,
      String embeddingApiKeyHeaderName,
      String embeddingAccessIdHeaderName,
      String embeddingsecretIdHeaderName) {
    this.tokenHeaderName = tokenHeaderName;
    this.embeddingApiKeyHeaderName = embeddingApiKeyHeaderName;
    this.embeddingAccessIdHeaderName = embeddingAccessIdHeaderName;
    this.embeddingsecretIdHeaderName = embeddingsecretIdHeaderName;
  }

  public EmbeddingCredentials create(
      RequestContext requestContext,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {

    var token = requestContext.getHttpHeaders().getHeader(this.tokenHeaderName);
    var embeddingApi = requestContext.getHttpHeaders().getHeader(this.embeddingApiKeyHeaderName);
    var accessId = requestContext.getHttpHeaders().getHeader(this.embeddingAccessIdHeaderName);
    var secretId = requestContext.getHttpHeaders().getHeader(this.embeddingsecretIdHeaderName);

    if (providerConfig == null) {
      return new EmbeddingCredentials(
          Optional.ofNullable(embeddingApi),
          Optional.ofNullable(accessId),
          Optional.ofNullable(secretId));
    }

    // if x-embedding-api-key is present, then use it, else use cassandraToken
    return new EmbeddingCredentials(
        providerConfig.authTokenPassThroughForNoneAuth()
            ? Optional.ofNullable(token)
            : Optional.ofNullable(embeddingApi),
        Optional.ofNullable(accessId),
        Optional.ofNullable(secretId));
  }
}
