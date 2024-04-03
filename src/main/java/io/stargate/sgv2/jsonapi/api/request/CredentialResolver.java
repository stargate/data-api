package io.stargate.sgv2.jsonapi.api.request;

import io.vertx.ext.web.RoutingContext;
import java.util.Optional;

/** Functional interface to resolve the cassandra username/password from the request context. */
@FunctionalInterface
public interface CredentialResolver {
  Optional<Credential> resolveCredential(RoutingContext context);

  record Credential(String userName, String password) {}
}
