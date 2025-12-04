package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;

/**
 * Implementation to resolve the embedding api key, access id and secret id from the request header.
 */
public class HeaderBasedEmbeddingCredentialsResolver implements EmbeddingCredentialsResolver {

  /**
   * Resolves the embedding api key, access id and secret id from the request header. Note, passing
   * in tenant since we do need tenant info for embedding metering.
   */
  public EmbeddingCredentials resolveEmbeddingCredentials(Tenant tenant, RoutingContext context) {
    HttpServerRequest request = context.request();
    String headerValue =
        request.getHeader(HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME);
    String accessId =
        request.getHeader(HttpConstants.EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME);
    String secretId =
        request.getHeader(HttpConstants.EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME);
    return new EmbeddingCredentials(
        tenant,
        Optional.ofNullable(headerValue),
        Optional.ofNullable(accessId),
        Optional.ofNullable(secretId));
  }
}
