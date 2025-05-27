package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import java.util.Objects;
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

  public EmbeddingCredentialsSupplier withAuthenticationConfig(
      EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationConfig authConfig) {
    this.authConfig = authConfig;
    return this;
  }

  public EmbeddingCredentials create(RequestContext requestContext) {
    Objects.requireNonNull(authConfig, "Authentication config cannot be null");

    String token = requestContext.getHttpHeaders().getHeader(this.tokenHeaderName);
    String embeddingApi = requestContext.getHttpHeaders().getHeader(this.embeddingApiKeyHeaderName);
    String accessId = requestContext.getHttpHeaders().getHeader(this.embeddingAccessIdHeaderName);
    String secretId = requestContext.getHttpHeaders().getHeader(this.embeddingsecretIdHeaderName);

    // if x-embedding-api-key is present, then use it, else use cassandraToken
    return new EmbeddingCredentials(
        authConfig.authTokenPassThrough()
            ? Optional.ofNullable(token)
            : Optional.ofNullable(embeddingApi),
        Optional.ofNullable(accessId),
        Optional.ofNullable(secretId));
  }
}
