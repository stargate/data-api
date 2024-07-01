package io.stargate.sgv2.jsonapi.api.request;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;

/** Functional interface to resolve the embedding api key from the request header. */
public class HeaderBasedEmbeddingApiKeysResolver implements EmbeddingCredentialResolver {
  private final String tokenHeaderName;
  private final String accessIdHeaderName;
  private final String secretIdHeaderName;

  public HeaderBasedEmbeddingApiKeysResolver(
      String tokenHeaderName, String accessIdHeaderName, String secretIdHeaderName) {
    this.tokenHeaderName = tokenHeaderName;
    this.accessIdHeaderName = accessIdHeaderName;
    this.secretIdHeaderName = secretIdHeaderName;
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
