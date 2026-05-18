/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.stargate.sgv2.jsonapi.api.security.gpuplane;

import io.quarkus.cache.CacheResult;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.config.GpuPlaneAuthConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pre-validates inbound API tokens against Astra control-plane endpoints, gated by {@link
 * GpuPlaneAuthConfig#enabled()}.
 *
 * <p>Routes by token format:
 *
 * <ul>
 *   <li>{@code AstraCS:[a-zA-Z]{24}:[a-f0-9]{64}} → GET {@code /v2/currentOrg}
 *   <li>three-part base64url JWT → POST {@code /v2/token/authorize} with the Insights-Plane
 *       basic-auth credentials
 * </ul>
 *
 * <p>The {@link #validate(String, String)} method is cached via Quarkus Caffeine ({@code
 * gpu-plane-tokens}) so a hot token does not hammer the upstream {@code api.datastax.com} on every
 * request — TTL is configured in {@code application.yaml}.
 */
@ApplicationScoped
public class GpuPlaneTokenValidator {

  private static final Logger logger = LoggerFactory.getLogger(GpuPlaneTokenValidator.class);

  private static final Pattern ASTRA_CS_PATTERN =
      Pattern.compile("^AstraCS:[a-zA-Z]{24}:[a-f0-9]{64}$");
  private static final Pattern JWT_PATTERN =
      Pattern.compile("^[A-Za-z0-9-_]+\\.[A-Za-z0-9-_]+\\.[A-Za-z0-9-_]+$");

  private static final String BEARER_PREFIX = "Bearer ";
  private static final String DEFAULT_DB_DRN = "drn:astra:org:*:db:*";
  private static final String DB_DRN_TEMPLATE = "drn:astra:org:*:db:%s";
  private static final String ACTION_ORG_DB_VIEW = "org-db-view";

  private final GpuPlaneAuthConfig config;
  private final AstraCurrentOrgClient currentOrgClient;
  private final AstraTokenAuthorizeClient tokenAuthorizeClient;

  @Inject
  public GpuPlaneTokenValidator(
      GpuPlaneAuthConfig config,
      @RestClient AstraCurrentOrgClient currentOrgClient,
      @RestClient AstraTokenAuthorizeClient tokenAuthorizeClient) {
    this.config = config;
    this.currentOrgClient = currentOrgClient;
    this.tokenAuthorizeClient = tokenAuthorizeClient;
  }

  /**
   * Validate {@code token} (optionally prefixed with {@code Bearer }) against Astra. {@code
   * tenantId} narrows the JWT-authorize DRN; pass {@code null} or empty to match any database.
   *
   * <p>Returns a {@link Result} whose {@link Result#allowed()} is true only if the upstream
   * confirms the token. {@link Result#orgId()} is populated on success.
   */
  @CacheResult(cacheName = "gpu-plane-tokens")
  public Uni<Result> validate(String token, String tenantId) {
    if (token == null || token.isEmpty()) {
      return Uni.createFrom().item(Result.deny("missing token"));
    }
    String stripped = stripBearer(token);
    if (ASTRA_CS_PATTERN.matcher(stripped).matches()) {
      return validateAstraCs(token);
    }
    if (JWT_PATTERN.matcher(stripped).matches()) {
      return validateJwt(stripped, tenantId);
    }
    return Uni.createFrom().item(Result.deny("unrecognized token format"));
  }

  private Uni<Result> validateAstraCs(String rawAuthorizationHeaderValue) {
    return currentOrgClient
        .currentOrg(rawAuthorizationHeaderValue)
        .map(
            response -> {
              if (response == null || response.id() == null || response.id().isEmpty()) {
                return Result.deny("currentOrg returned no id");
              }
              return Result.allow(response.id(), TokenType.ASTRA_CS);
            })
        .onFailure()
        .recoverWithItem(
            err -> {
              logger.debug("AstraCS validation failed", err);
              return Result.deny("AstraCS rejected by upstream");
            });
  }

  private Uni<Result> validateJwt(String bareToken, String tenantId) {
    String basicAuth = basicAuthHeader();
    if (basicAuth == null) {
      logger.warn(
          "GPU-plane auth enabled but Insights-Plane credentials are not configured; denying");
      return Uni.createFrom().item(Result.deny("missing insights-plane credentials"));
    }
    String resource =
        (tenantId == null || tenantId.isBlank())
            ? DEFAULT_DB_DRN
            : DB_DRN_TEMPLATE.formatted(tenantId);
    AstraTokenAuthorizeClient.AuthorizeRequest body =
        new AstraTokenAuthorizeClient.AuthorizeRequest(
            bareToken, List.of(ACTION_ORG_DB_VIEW), List.of(resource));
    return tokenAuthorizeClient
        .authorize(basicAuth, "true", body)
        .map(
            response -> {
              if (response == null || response.orgId() == null || response.orgId().isEmpty()) {
                return Result.deny("token/authorize returned no org_id");
              }
              if (!response.allow()) {
                return Result.deny("token/authorize allow=false");
              }
              return Result.allow(response.orgId(), TokenType.JWT);
            })
        .onFailure()
        .recoverWithItem(
            err -> {
              logger.debug("JWT validation failed", err);
              return Result.deny("JWT rejected by upstream");
            });
  }

  private String basicAuthHeader() {
    String user = config.insightsPlaneUsername().orElse(null);
    String pass = config.insightsPlanePassword().orElse(null);
    if (user == null || user.isEmpty() || pass == null || pass.isEmpty()) {
      return null;
    }
    String raw = user + ":" + pass;
    String encoded = Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    return "Basic " + encoded;
  }

  static String stripBearer(String token) {
    if (token != null && token.startsWith(BEARER_PREFIX)) {
      return token.substring(BEARER_PREFIX.length());
    }
    return token;
  }

  /** Outcome of a validation call. */
  public record Result(boolean allowed, String orgId, TokenType tokenType, String denyReason) {
    public static Result allow(String orgId, TokenType type) {
      return new Result(true, orgId, type, null);
    }

    public static Result deny(String reason) {
      return new Result(false, null, null, reason);
    }
  }

  public enum TokenType {
    ASTRA_CS,
    JWT
  }
}
