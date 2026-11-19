package io.stargate.sgv2.jsonapi.api.security;

import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.vertx.http.runtime.security.HttpSecurityPolicy;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;

/** Checks which mechanism supplied the identity before the route's authorization policy runs. */
@ApplicationScoped
public class DataApiAuthenticationPolicy implements HttpSecurityPolicy {
  static final String NAME = "data-api";
  private static final String ATTRIBUTE = DataApiAuthenticationPolicy.class.getName();
  private static final Object VERIFIED = new Object();

  static SecurityIdentity authenticatedIdentity(SecurityIdentity identity) {
    return QuarkusSecurityIdentity.builder(identity)
        .addPermissions(identity.getPermissions())
        .addAttribute(ATTRIBUTE, VERIFIED)
        .build();
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Uni<CheckResult> checkPermission(
      RoutingContext context, Uni<SecurityIdentity> identity, AuthorizationRequestContext request) {
    // A competing shared HTTP permission may have selected another mechanism.
    return identity.map(
        id ->
            id.isAnonymous() || id.getAttribute(ATTRIBUTE) == VERIFIED
                ? CheckResult.PERMIT
                : CheckResult.DENY);
  }
}
