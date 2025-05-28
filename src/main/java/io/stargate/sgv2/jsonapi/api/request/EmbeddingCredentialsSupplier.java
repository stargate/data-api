package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import java.util.Map;
import java.util.Optional;

public class EmbeddingCredentialsSupplier {
  private final String authTokenHeaderName;
  private final String embeddingApiKeyHeaderName;
  private final String embeddingAccessIdHeaderName;
  private final String embeddingsecretIdHeaderName;
  private Map<String, String> authConfigFromCollection;

  public EmbeddingCredentialsSupplier(
      String authTokenHeaderName,
      String embeddingApiKeyHeaderName,
      String embeddingAccessIdHeaderName,
      String embeddingsecretIdHeaderName) {
    this.authTokenHeaderName = authTokenHeaderName;
    this.embeddingApiKeyHeaderName = embeddingApiKeyHeaderName;
    this.embeddingAccessIdHeaderName = embeddingAccessIdHeaderName;
    this.embeddingsecretIdHeaderName = embeddingsecretIdHeaderName;
  }

  public void withAuthConfigFromCollection(Map<String, String> authConfigFromCollection) {
    this.authConfigFromCollection = authConfigFromCollection;
  }

  public EmbeddingCredentials create(
      RequestContext requestContext,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {

    var authToken = requestContext.getHttpHeaders().getHeader(this.authTokenHeaderName);
    var embeddingApi = requestContext.getHttpHeaders().getHeader(this.embeddingApiKeyHeaderName);
    var accessId = requestContext.getHttpHeaders().getHeader(this.embeddingAccessIdHeaderName);
    var secretId = requestContext.getHttpHeaders().getHeader(this.embeddingsecretIdHeaderName);

    // If these three conditions are met, we use the auth token as the embeddingApiKey:
    // 1. user didn't provide x-embedding-api-key
    // 2. Collection supports NONE auth (in the yaml config and authConfigFromCollection is null)
    // 3. Provider has authTokenPassThroughForNoneAuth set to true
    if (embeddingApi == null
        && providerConfig != null
        && providerConfig
            .supportedAuthentications()
            .containsKey(EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType.NONE)
        && providerConfig
            .supportedAuthentications()
            .get(EmbeddingProvidersConfig.EmbeddingProviderConfig.AuthenticationType.NONE)
            .enabled()
        && authConfigFromCollection == null
        && providerConfig.authTokenPassThroughForNoneAuth()) {
      return new EmbeddingCredentials(
          Optional.ofNullable(authToken), Optional.empty(), Optional.empty());
    }

    return new EmbeddingCredentials(
        Optional.ofNullable(embeddingApi),
        Optional.ofNullable(accessId),
        Optional.ofNullable(secretId));
  }
}
