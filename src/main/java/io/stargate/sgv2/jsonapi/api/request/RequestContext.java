package io.stargate.sgv2.jsonapi.api.request;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.request.tenant.DataApiTenantResolver;
import io.stargate.sgv2.jsonapi.api.request.token.DataApiTokenResolver;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.Optional;

/**
 * This class is used to get the request info like tenantId, cassandraToken and embeddingApiKey.
 * This is a replacement to StargateRequestInfo so bridge connection is removed.
 *
 * <p><b>Note:</b> aaron feb 3 2025 - leaving with injection for now for the building, it may make
 * sense but moving it to be part of the CommandContext rather than handed around everywhere.
 */
@RequestScoped
public class RequestContext {

  private static final NoArgGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochGenerator();

  private final Optional<String> tenantId;
  private final Optional<String> cassandraToken;
  private final EmbeddingCredentialsSupplier embeddingCredentialsSupplier;
  private final RerankingCredentials rerankingCredentials;
  private final HttpHeaderAccess httpHeaders;
  private final String requestId;

  private final String userAgent;

  /** FOR TESTING ONLY - so we can bypass pulling things the headers, still messy, getting better */
  @VisibleForTesting
  public RequestContext(
      Optional<String> tenantId,
      Optional<String> cassandraToken,
      RerankingCredentials rerankingCredentials,
      String userAgent) {
    this.tenantId = tenantId;
    this.cassandraToken = cassandraToken;
    embeddingCredentialsSupplier =
        new EmbeddingCredentialsSupplier(
            HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME,
            HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
            HttpConstants.EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME,
            HttpConstants.EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME);
    this.rerankingCredentials = rerankingCredentials;
    this.userAgent = userAgent;
    this.httpHeaders = new HttpHeaderAccess(io.vertx.core.MultiMap.caseInsensitiveMultiMap());
    requestId = generateRequestId();
  }

  @Inject
  public RequestContext(
      RoutingContext routingContext,
      SecurityContext securityContext,
      Instance<DataApiTenantResolver> tenantResolver,
      Instance<DataApiTokenResolver> tokenResolver,
      HttpConstants httpConstants) {

    tenantId = tenantResolver.get().resolve(routingContext, securityContext);
    cassandraToken = tokenResolver.get().resolve(routingContext, securityContext);
    httpHeaders = new HttpHeaderAccess(routingContext.request().headers());
    requestId = generateRequestId();
    userAgent = httpHeaders.getHeader(HttpHeaders.USER_AGENT);

    embeddingCredentialsSupplier =
        new EmbeddingCredentialsSupplier(
            httpConstants.authToken(),
            httpConstants.embeddingApiKey(),
            httpConstants.embeddingAccessId(),
            httpConstants.embeddingSecretId());

    // if x-reranking-api-key is present, then use it, else use cassandraToken
    Optional<String> rerankingApiKeyFromHeader =
        HeaderBasedRerankingKeyResolver.resolveRerankingKey(routingContext);
    rerankingCredentials =
        rerankingApiKeyFromHeader
            .map(apiKey -> new RerankingCredentials(this.tenantId.orElse(""), Optional.of(apiKey)))
            .orElse(
                this.cassandraToken
                    .map(
                        cassandraToken ->
                            new RerankingCredentials(
                                this.tenantId.orElse(""), Optional.of(cassandraToken)))
                    .orElse(new RerankingCredentials(this.tenantId.orElse(""), Optional.empty())));
  }

  private static String generateRequestId() {
    return UUID_V7_GENERATOR.generate().toString();
  }

  public String getRequestId() {
    return requestId;
  }

  public Optional<String> getTenantId() {
    return tenantId;
  }

  public Optional<String> getCassandraToken() {
    return cassandraToken;
  }

  public Optional<String> getUserAgent() {
    return Optional.ofNullable(userAgent);
  }

  public EmbeddingCredentialsSupplier getEmbeddingCredentialsSupplier() {
    return embeddingCredentialsSupplier;
  }

  public RerankingCredentials getRerankingCredentials() {
    return rerankingCredentials;
  }

  public HttpHeaderAccess getHttpHeaders() {
    return this.httpHeaders;
  }

  /**
   * Simple wrapper around internal HTTP header container, providing safe(r) access to typed header
   * values. Minimal API, currently mainly used for feature flags.
   */
  public static class HttpHeaderAccess {
    private final io.vertx.core.MultiMap headers;

    public HttpHeaderAccess(io.vertx.core.MultiMap headers) {
      this.headers = headers;
    }

    public String getHeader(String headerName) {
      return headers.get(headerName);
    }

    public List<String> getHeaders(String headerName) {
      return headers.getAll(headerName);
    }

    /**
     * Accessor for getting value of given header, as {@code Boolean} if (and only if!) value is one
     * of "true" or "false". Access by name is (and has to be) case-insensitive as per HTTP
     * standard.
     *
     * @param headerName Name of header to check
     * @return Boolean.TRUE if header value is "true", Boolean.FALSE if "false", or null if not
     */
    public Boolean getHeaderAsBoolean(String headerName) {
      String str = headers.get(headerName);
      // Only consider strict "true" and "false"; ignore other values
      if ("true".equals(str)) {
        return Boolean.TRUE;
      }
      if ("false".equals(str)) {
        return Boolean.FALSE;
      }
      return null;
    }
  }
}
