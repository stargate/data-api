package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * Implementation to resolve the embedding api key, access id and secret id from the request header.
 */
@Singleton
public class HeaderBasedRerankCredentialsResolver implements RerankCredentialsResolver {

  public RerankCredentials resolveRerankCredentials(RoutingContext context) {
    HttpServerRequest request = context.request();
    String token = request.getHeader(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME);
    String legacyToken =
        request.getHeader(HttpConstants.DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME);

    return new RerankCredentials(
        Optional.ofNullable(token != null ? token : legacyToken), Optional.empty());
  }
}
