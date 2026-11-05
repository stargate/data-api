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

package io.stargate.sgv2.jsonapi.api.security;

import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME;
import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.smallrye.jwt.runtime.auth.JsonWebTokenCredential;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.quarkus.vertx.http.runtime.security.HttpCredentialTransport;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.security.challenge.impl.ErrorChallengeSender;
import io.stargate.sgv2.jsonapi.config.AuthConfig;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.util.Optional;
import java.util.Set;

/**
 * Coordinates OAuth-only MCP requests and the existing Data API token headers.
 *
 * @see HeaderIdentityProvider
 */
public class HeaderBasedAuthenticationMechanism implements HttpAuthenticationMechanism {

  /** The name of the header to be used for the authentication. */
  private final String headerName;

  private final boolean headerEnabled;
  private final boolean bearerEnabled;
  private final McpOAuthMetadata metadata;

  /** Customize the challenge. */
  private final Instance<ErrorChallengeSender> customChallengeSender;

  public HeaderBasedAuthenticationMechanism(
      AuthConfig config,
      Instance<ErrorChallengeSender> customChallengeSender,
      McpOAuthMetadata metadata) {
    this.headerName = config.headerBased().headerName();
    this.headerEnabled = config.headerBased().enabled();
    this.bearerEnabled = config.bearer().enabled();
    this.customChallengeSender = customChallengeSender;
    this.metadata = metadata;
  }

  @Override
  public Uni<SecurityIdentity> authenticate(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    return authenticateCredentials(context, identityProviderManager)
        .onFailure(AuthenticationFailedException.class)
        .invoke(
            failure ->
                // The MCP transport writes authentication failures without requesting a challenge.
                metadata
                    .challenge(context)
                    .ifPresent(
                        challenge ->
                            context.response().putHeader(HttpHeaders.WWW_AUTHENTICATE, challenge)));
  }

  private Uni<SecurityIdentity> authenticateCredentials(
      RoutingContext context, IdentityProviderManager identityProviderManager) {
    String authorization = context.request().getHeader(HttpHeaders.AUTHORIZATION);
    if (bearerEnabled && metadata.isMcpRequest(context)) {
      context.put(HttpAuthenticationMechanism.class.getName(), this);
      if (authorization == null
          || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)
          || authorization.substring(7).isBlank()) {
        return Uni.createFrom().failure(new AuthenticationFailedException());
      }
      var request =
          new TokenAuthenticationRequest(
              new JsonWebTokenCredential(authorization.substring(7).trim()));
      HttpSecurityUtils.setRoutingContextAttribute(request, context);
      return identityProviderManager.authenticate(request);
    }

    if (headerEnabled) {
      String headerValue = legacyHeader(context);
      if (headerValue != null) {
        context.put(HttpAuthenticationMechanism.class.getName(), this);
        var request = new HeaderAuthenticationRequest(headerName, headerValue);
        HttpSecurityUtils.setRoutingContextAttribute(request, context);
        return identityProviderManager.authenticate(request);
      }
    }

    // Do not let the lower-priority JWT mechanism enable Bearer authentication implicitly.
    if (authorization != null) {
      context.put(HttpAuthenticationMechanism.class.getName(), this);
      return Uni.createFrom().failure(new AuthenticationFailedException());
    }
    return Uni.createFrom().optional(Optional.empty());
  }

  private String legacyHeader(RoutingContext context) {
    // Data API Authentication header key: Token
    String headerValue = context.request().getHeader(headerName);
    // Data API Authentication header key: token
    if (null == headerValue) {
      headerValue = context.request().getHeader(AUTHENTICATION_TOKEN_HEADER_NAME.toLowerCase());
    }
    // Data API supports X-Cassandra-Token for backward compatibility.
    if (null == headerValue) {
      headerValue = context.request().getHeader(DEPRECATED_AUTHENTICATION_TOKEN_HEADER_NAME);
    }
    return headerValue;
  }

  @Override
  public int getPriority() {
    // Keep the feature flag ahead of the built-in JWT mechanism.
    return 3000;
  }

  /** {@inheritDoc} */
  @Override
  public Uni<ChallengeData> getChallenge(RoutingContext context) {
    var challenge =
        metadata
            .challenge(context)
            .map(
                value ->
                    new ChallengeData(
                        Response.Status.UNAUTHORIZED.getStatusCode(),
                        HttpHeaders.WWW_AUTHENTICATE,
                        value))
            .orElseGet(
                () -> new ChallengeData(Response.Status.UNAUTHORIZED.getStatusCode(), null, null));
    return Uni.createFrom().item(challenge);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Writes a custom response.
   */
  @Override
  public Uni<Boolean> sendChallenge(RoutingContext context) {
    // if we should not customize use default
    if (!customChallengeSender.isResolvable()) {
      return HttpAuthenticationMechanism.super.sendChallenge(context);
    }
    return getChallenge(context).flatMap(c -> customChallengeSender.get().apply(context, c));
  }

  /** {@inheritDoc} */
  @Override
  public Set<Class<? extends AuthenticationRequest>> getCredentialTypes() {
    return Set.of(HeaderAuthenticationRequest.class, TokenAuthenticationRequest.class);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<HttpCredentialTransport> getCredentialTransport(RoutingContext context) {
    HttpCredentialTransport transport =
        new HttpCredentialTransport(HttpCredentialTransport.Type.OTHER_HEADER, headerName);
    return Uni.createFrom().item(transport);
  }
}
