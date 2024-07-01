package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.api.request.tenant.DataApiTenantResolver;
import io.stargate.sgv2.jsonapi.api.request.token.DataApiTokenResolver;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Optional;

/**
 * This class is used to get the request info like tenantId, cassandraToken and embeddingApiKey.
 * This is a replacement to StargateRequestInfo so bridge connection is removed.
 */
@RequestScoped
public class DataApiRequestInfo {
  private final Optional<String> tenantId;
  private final Optional<String> cassandraToken;
  private EmbeddingProvider.Credentials credentials;

  /**
   * Constructor that will be useful in the offline library mode, where only the tenant will be set
   * and accessed.
   *
   * @param tenantId Tenant Id
   */
  public DataApiRequestInfo(Optional<String> tenantId) {
    this.tenantId = tenantId;
    this.cassandraToken = Optional.empty();
    this.credentials = null;
  }

  @Inject
  public DataApiRequestInfo(
      RoutingContext routingContext,
      SecurityContext securityContext,
      Instance<DataApiTenantResolver> tenantResolver,
      Instance<DataApiTokenResolver> tokenResolver,
      Instance<EmbeddingApiKeyResolver> apiKeyResolver) {
    // TODO: replace the null once support accessKeyId and secretAccessKey header
    this.credentials =
        new EmbeddingProvider.Credentials(
            apiKeyResolver.get().resolveApiKey(routingContext), Optional.empty(), Optional.empty());
    this.tenantId = (tenantResolver.get()).resolve(routingContext, securityContext);
    this.cassandraToken = (tokenResolver.get()).resolve(routingContext, securityContext);
  }

  public Optional<String> getTenantId() {
    return this.tenantId;
  }

  public Optional<String> getCassandraToken() {
    return this.cassandraToken;
  }

  public EmbeddingProvider.Credentials getCredentials() {
    return this.credentials;
  }
}
