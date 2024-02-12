package io.stargate.sgv2.jsonapi.api.request;

import io.vertx.ext.web.RoutingContext;
import java.util.Optional;

@FunctionalInterface
public interface EmbeddingApiKeyResolver {
  Optional<String> resolveApiKey(RoutingContext context);
}
