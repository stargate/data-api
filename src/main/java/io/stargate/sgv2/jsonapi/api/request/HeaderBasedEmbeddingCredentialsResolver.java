package io.stargate.sgv2.jsonapi.api.request;

/**
 * Implementation to resolve the embedding api key, access id and secret id from the request header.
 */
// public class HeaderBasedEmbeddingCredentialsResolver implements EmbeddingCredentialsResolver {
//  private final String tokenHeaderName;
//  private final String accessIdHeaderName;
//  private final String secretIdHeaderName;
//
//  public HeaderBasedEmbeddingCredentialsResolver(
//      String tokenHeaderName, String accessIdHeaderName, String secretIdHeaderName) {
//    this.tokenHeaderName =
//        Objects.requireNonNull(tokenHeaderName, "Token header name cannot be null");
//    this.accessIdHeaderName =
//        Objects.requireNonNull(accessIdHeaderName, "Access Id header name cannot be null");
//    this.secretIdHeaderName =
//        Objects.requireNonNull(secretIdHeaderName, "Secret Id header name cannot be null");
//  }
//
//  public EmbeddingCredentials resolveEmbeddingCredentials(RoutingContext context) {
//    HttpServerRequest request = context.request();
//    String headerValue = request.getHeader(this.tokenHeaderName);
//    String accessId = request.getHeader(this.accessIdHeaderName);
//    String secretId = request.getHeader(this.secretIdHeaderName);
//    return new EmbeddingCredentials(
//        Optional.ofNullable(headerValue),
//        Optional.ofNullable(accessId),
//        Optional.ofNullable(secretId));
//  }
// }
