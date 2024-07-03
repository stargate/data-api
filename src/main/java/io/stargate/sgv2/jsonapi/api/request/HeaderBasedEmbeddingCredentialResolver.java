package io.stargate.sgv2.jsonapi.api.request;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Objects;
import java.util.Optional;

/**
 * Implementation to resolve the embedding api key, access id and secret id from the request header.
 */
public class HeaderBasedEmbeddingCredentialResolver implements EmbeddingCredentialResolver {
  private final String tokenHeaderName;
  private final String accessIdHeaderName;
  private final String secretIdHeaderName;

  public HeaderBasedEmbeddingCredentialResolver(
      String tokenHeaderName, String accessIdHeaderName, String secretIdHeaderName) {
    this.tokenHeaderName =
        Objects.requireNonNull(tokenHeaderName, "Token header name cannot be null");
    this.accessIdHeaderName =
        Objects.requireNonNull(accessIdHeaderName, "Access Id header name cannot be null");
    this.secretIdHeaderName =
        Objects.requireNonNull(secretIdHeaderName, "Secret Id header name cannot be null");
  }

  public EmbeddingCredential resolveEmbeddingCredential(RoutingContext context) {
    HttpServerRequest request = context.request();
    String headerValue = request.getHeader(this.tokenHeaderName);
    String accessId = request.getHeader(this.accessIdHeaderName);
    String secretId = request.getHeader(this.secretIdHeaderName);
    return new EmbeddingCredential(
        Optional.ofNullable(headerValue),
        Optional.ofNullable(accessId),
        Optional.ofNullable(secretId));
  }
}
