package io.stargate.sgv2.jsonapi.api.request;

import io.vertx.ext.web.RoutingContext;
import java.util.Optional;

/** Functional interface to resolve the embedding api key from the request context. */
@FunctionalInterface
public interface EmbeddingCredentialResolver {
  Optional<EmbeddingCredential> resolveEmbeddingCredential(RoutingContext context);
}
