package io.stargate.sgv2.jsonapi.api.request.token;

import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME;

import io.quarkus.vertx.http.runtime.filters.Filters;
import io.vertx.core.MultiMap;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.event.Observes;

/**
 * Request based, filtering out authentication token and add correct header Coordinator needs
 * X-Cassandra-Token as token name JSON API requires Token as token name Also supports
 * X-Cassandra-Token for backward compatibility
 */
@RequestScoped
public class TokenFilter {

  // JSON API token name "Token", also supports X-Cassandra-Token for backward compatibility
  public void registerMyFilter(@Observes Filters filters) {
    filters.register(
        rc -> {
          MultiMap headers = rc.request().headers();
          if (headers.contains("Token")) {
            headers.add(DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME, headers.get("Token"));
          } else if (headers.contains("token")) {
            headers.add(DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME, headers.get("token"));
          }
          rc.next();
        },
        10000);
  }
}
