package io.stargate.sgv2.jsonapi.api.request;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;

/** Functional interface to resolve the embedding api key from the request header. */
public class HeaderBasedEmbeddingApiKeyResolver implements EmbeddingCredentialResolver {
  private final String tokenHeaderName;
  private final String accessIdHeaderName;
  private final String secretIdHeaderName;

  public HeaderBasedEmbeddingApiKeyResolver(
      String tokenHeaderName, String accessIdHeaderName, String secretIdHeaderName) {
    this.tokenHeaderName = tokenHeaderName;
    this.accessIdHeaderName = accessIdHeaderName;
    this.secretIdHeaderName = secretIdHeaderName;
  }

  public Optional<EmbeddingCredential> resolveEmbeddingCredential(RoutingContext context) {
    HttpServerRequest request = context.request();
    String headerValue = request.getHeader(this.tokenHeaderName);
    String accessId = request.getHeader(this.accessIdHeaderName);
    String secretId = request.getHeader(this.secretIdHeaderName);
    return Optional.ofNullable(
        new EmbeddingCredential(
            Optional.ofNullable(headerValue),
            Optional.ofNullable(accessId),
            Optional.ofNullable(secretId)));
  }
}
