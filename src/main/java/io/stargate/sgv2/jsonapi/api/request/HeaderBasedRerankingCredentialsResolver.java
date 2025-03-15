package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * Implementation to resolve the embedding api key, access id and secret id from the request header.
 *
 * <p>We need to store both Data API token and Reranking Token in the RerankingCredentials record.
 * E.G. For Astra self-hosted Nvidia rerank in the GPU plane, it requires the AstraCS token to
 * access, so Data API in Astra will resolve the AstraCS token from the request header.
 *
 * <p>For Data API in non-astra environment, since the token is also used for backend
 * authentication, so the user needs to pass the rerank API key in the request header
 * 'x-rerank-api-key'.
 */
@Singleton
public class HeaderBasedRerankingCredentialsResolver implements RerankingCredentialsResolver {

  public RerankingCredentials resolveRerankingCredentials(RoutingContext context) {
    HttpServerRequest request = context.request();
    String token = request.getHeader(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME);
    String legacyToken =
        request.getHeader(HttpConstants.DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME);
    String rerankingApiKey =
        request.getHeader(HttpConstants.RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME);
    ;

    return new RerankingCredentials(
        token != null ? token : legacyToken, Optional.ofNullable(rerankingApiKey));
  }
}
