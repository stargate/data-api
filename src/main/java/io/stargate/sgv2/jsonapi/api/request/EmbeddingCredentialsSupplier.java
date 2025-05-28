package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import java.util.Map;
import java.util.Optional;

/**
 * A supplier for creating {@link EmbeddingCredentials} based on the current request context,
 * collection authentication configuration, and embedding provider configuration.
 *
 * <p>This class centralizes the logic for determining which credentials to use for embedding
 * service calls.
 */
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

  /** Sets the authentication configuration defined at the createCollection command. */
  public void withAuthConfigFromCollection(Map<String, String> authConfigFromCollection) {
    this.authConfigFromCollection = authConfigFromCollection;
  }

  /**
   * Creates an {@link EmbeddingCredentials} instance based on the current request context and
   * provider configuration.
   *
   * @param requestContext The current request context containing HTTP headers.
   * @param providerConfig The configuration for the embedding provider.
   * @return An instance of {@link EmbeddingCredentials} with the appropriate credentials.
   */
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
