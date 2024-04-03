package io.stargate.sgv2.jsonapi.api.request;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;

/** Functional interface to resolve the cassandra username/password from the request header. */
public class HeaderBasedCredentialResolver implements CredentialResolver {
  private final String userNameHeader;
  private final String passwordHeader;

  public HeaderBasedCredentialResolver(String userNameHeader, String passwordHeader) {
    this.userNameHeader = userNameHeader;
    this.passwordHeader = passwordHeader;
  }

  public Optional<Credential> resolveCredential(RoutingContext context) {
    HttpServerRequest request = context.request();
    String userNameValue = request.getHeader(this.userNameHeader);
    String passwordValue = request.getHeader(this.passwordHeader);
    if (userNameValue == null || passwordValue == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(new Credential(userNameValue, passwordValue));
  }
}
