package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.grpc.RetriableStargateBridge;
import io.stargate.sgv2.api.common.tenant.TenantResolver;
import io.stargate.sgv2.api.common.token.CassandraTokenResolver;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Optional;

@RequestScoped
public class DataApiRequestInfo extends StargateRequestInfo {
  private final Optional<String> embeddingApiKey;

  @Inject
  public DataApiRequestInfo(
      RoutingContext routingContext,
      SecurityContext securityContext,
      RetriableStargateBridge bridge,
      Instance<TenantResolver> tenantResolver,
      Instance<CassandraTokenResolver> tokenResolver,
      Instance<EmbeddingApiKeyResolver> apiKeyResolver) {
    super(routingContext, securityContext, bridge, tenantResolver, tokenResolver);
    this.embeddingApiKey = apiKeyResolver.get().resolveApiKey(routingContext);
  }

  public Optional<String> getEmbeddingApiKey() {
    return this.embeddingApiKey;
  }
}
