package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.inject.Singleton;
import java.util.Optional;

/**
 * We need to store both Data API token and Reranking Token in the RerankingCredentials record. E.G.
 * For Astra self-hosted Nvidia reranking in the GPU plane, it requires the AstraCS token to access,
 * so Data API in Astra will resolve the AstraCS token from the request header 'reranking-api-key'.
 * If it is not present, then cassandra token will be used as the reranking api key. see {@link
 * RequestContext}
 *
 * <p>For Data API in non-astra environment, since the token is also used for backend
 * authentication, so the user needs to pass the reranking API key in the request header
 * 'reranking-api-key'.
 */
@Singleton
public class HeaderBasedRerankingKeyResolver {

  public Optional<String> resolveRerankingKey(RoutingContext context) {
    HttpServerRequest request = context.request();
    String rerankingApiKey =
        request.getHeader(HttpConstants.RERANKING_AUTHENTICATION_TOKEN_HEADER_NAME);
    return Optional.ofNullable(rerankingApiKey);
  }
}
