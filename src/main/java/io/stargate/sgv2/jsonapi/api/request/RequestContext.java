package io.stargate.sgv2.jsonapi.api.request;

import static io.stargate.sgv2.jsonapi.util.StringUtil.normalizeOptionalString;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.request.tenant.RequestTenantResolver;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.api.request.token.RequestAuthTokenResolver;
import io.stargate.sgv2.jsonapi.logging.LoggingMDCContext;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.SecurityContext;
import java.util.List;
import java.util.Objects;
import org.slf4j.MDC;

/**
 * This class is used to get the request info like tenantId, cassandraToken and embeddingApiKey.
 * This is a replacement to StargateRequestInfo so bridge connection is removed.
 *
 * <p><b>Note:</b> aaron feb 3 2025 - leaving with injection for now for the building, it may make
 * sense but moving it to be part of the CommandContext rather than handed around everywhere.
 */
@RequestScoped
public class RequestContext implements LoggingMDCContext {

  private static final NoArgGenerator UUID_V7_GENERATOR = Generators.timeBasedEpochGenerator();

  private final String authToken;
  private final String requestId;
  private final UserAgent userAgent;
  private final Tenant tenant;

  private final EmbeddingCredentials embeddingCredentials;
  private final HttpHeaderAccess httpHeaders;
  private final RerankingCredentials rerankingCredentials;

  /** For testing purposes only. */
  @VisibleForTesting
  public RequestContext(Tenant tenant) {

    this.authToken = normalizeOptionalString((String) null);
    this.requestId = generateRequestId();
    this.userAgent = new UserAgent(null);
    this.tenant = Objects.requireNonNull(tenant, "tenant must not be null");

    this.embeddingCredentials = null;
    this.rerankingCredentials = null;
    this.httpHeaders = null;
  }

  @Inject
  public RequestContext(
      RoutingContext routingContext,
      SecurityContext securityContext,
      Instance<RequestTenantResolver> tenantResolver,
      Instance<RequestAuthTokenResolver> tokenResolver,
      Instance<EmbeddingCredentialsResolver> embeddingCredentialsResolver) {

    this.httpHeaders = new HttpHeaderAccess(routingContext.request().headers());

    this.authToken = tokenResolver.get().resolve(routingContext, securityContext);
    this.requestId = generateRequestId();
    this.userAgent = new UserAgent(httpHeaders.getHeader(HttpHeaders.USER_AGENT));
    this.tenant = tenantResolver.get().resolve(routingContext, securityContext);

    this.embeddingCredentials =
        embeddingCredentialsResolver.get().resolveEmbeddingCredentials(routingContext);
    // user specified the reranking key in the request header, use that.
    // fall back to whatever they provided as the auth token for the API
    this.rerankingCredentials =
        HeaderBasedRerankingKeyResolver.resolveRerankingKey(routingContext)
            .map(s -> new RerankingCredentials(normalizeOptionalString(s)))
            .orElseGet(() -> new RerankingCredentials(normalizeOptionalString(this.authToken)));
  }

  private static String generateRequestId() {
    return UUID_V7_GENERATOR.generate().toString();
  }

  public String getRequestId() {
    return requestId;
  }

  /**
   * @return Non-null {@link Tenant} object
   */
  public Tenant getTenant() {
    return tenant;
  }

  /**
   * @return Non-null authToken from the request processed with {@link
   *     io.stargate.sgv2.jsonapi.util.StringUtil#normalizeOptionalString(String)}
   */
  public String getAuthToken() {
    return authToken;
  }

  /**
   * @return Non-null userAgent from the request processed with {@link
   *     io.stargate.sgv2.jsonapi.util.StringUtil#normalizeOptionalString(String)}
   */
  public UserAgent getUserAgent() {
    return userAgent;
  }

  public EmbeddingCredentials getEmbeddingCredentials() {
    return embeddingCredentials;
  }

  public RerankingCredentials getRerankingCredentials() {
    return rerankingCredentials;
  }

  public HttpHeaderAccess getHttpHeaders() {
    return this.httpHeaders;
  }

  @Override
  public void addToMDC() {
    MDC.put("tenantId", tenant.toString());
  }

  @Override
  public void removeFromMDC() {
    MDC.remove("tenantId");
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
