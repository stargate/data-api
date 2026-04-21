package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.vertx.ext.web.RoutingContext;

/** Functional interface to resolve the embedding api key from the request context. */
@FunctionalInterface
public interface EmbeddingCredentialsResolver {
  EmbeddingCredentials resolveEmbeddingCredentials(Tenant tenant, RoutingContext context);
}
