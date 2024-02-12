package io.stargate.sgv2.jsonapi.api.request;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;

public class HeaderBasedEmbeddingApiKeyResolver implements EmbeddingApiKeyResolver {
  private final String headerName;

  public HeaderBasedEmbeddingApiKeyResolver(String headerName) {
    this.headerName = headerName;
  }

  public Optional<String> resolveApiKey(RoutingContext context) {
    HttpServerRequest request = context.request();
    String headerValue = request.getHeader(this.headerName);
    return Optional.ofNullable(headerValue);
  }
}
