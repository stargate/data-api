package io.stargate.sgv2.jsonapi.api.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.AuthenticationRequest;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.security.challenge.impl.ErrorChallengeSender;
import io.stargate.sgv2.jsonapi.config.AuthConfig;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.inject.Instance;
import jakarta.ws.rs.core.HttpHeaders;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class HeaderBasedAuthenticationMechanismTest {

  @Test
  void bearerCredentialsReachTheJwtProviderBeforeLegacyHeaders() {
    var manager = mock(IdentityProviderManager.class);
    var identity = mock(SecurityIdentity.class);
    when(manager.authenticate(any())).thenReturn(Uni.createFrom().item(identity));
    var mechanism = mechanism(true, true);
    var context = context(Map.of("Authorization", "Bearer encoded-jwt", "Token", "legacy-token"));

    assertThat(mechanism.authenticate(context, manager).await().indefinitely()).isSameAs(identity);

    var request = ArgumentCaptor.forClass(AuthenticationRequest.class);
    verify(manager).authenticate(request.capture());
    assertThat(request.getValue()).isInstanceOf(TokenAuthenticationRequest.class);
    assertThat(((TokenAuthenticationRequest) request.getValue()).getToken().getToken())
        .isEqualTo("encoded-jwt");
    verify(context).put(HttpAuthenticationMechanism.class.getName(), mechanism);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Bearer", "Bearer ", "Bearer\tencoded-jwt", "Basic encoded"})
  void malformedAuthorizationCannotFallBackToALegacyHeader(String authorization) {
    var manager = mock(IdentityProviderManager.class);
    var context = context(Map.of("Authorization", authorization, "Token", "legacy-token"));
    var mechanism = mechanism(true, true);

    assertThatThrownBy(() -> mechanism.authenticate(context, manager).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class);
    verify(context).put(HttpAuthenticationMechanism.class.getName(), mechanism);
    verifyNoInteractions(manager);
  }

  @Test
  void providerFailureCannotFallBackToALegacyHeader() {
    var manager = mock(IdentityProviderManager.class);
    when(manager.authenticate(any()))
        .thenReturn(Uni.createFrom().failure(new AuthenticationFailedException()));
    var context = context(Map.of("Authorization", "Bearer invalid-jwt", "Token", "legacy-token"));
    var mechanism = mechanism(true, true);

    assertThatThrownBy(() -> mechanism.authenticate(context, manager).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class);
    verify(context).put(HttpAuthenticationMechanism.class.getName(), mechanism);
    var request = ArgumentCaptor.forClass(AuthenticationRequest.class);
    verify(manager).authenticate(request.capture());
    assertThat(request.getValue()).isInstanceOf(TokenAuthenticationRequest.class);
  }

  @Test
  void rejectedBearerIncludesTheChallengeBeforeTheMcpTransportWritesItsError() {
    var manager = mock(IdentityProviderManager.class);
    when(manager.authenticate(any()))
        .thenReturn(Uni.createFrom().failure(new AuthenticationFailedException()));
    var context = context(Map.of("Authorization", "Bearer invalid-jwt"));
    var response = mock(HttpServerResponse.class);
    when(context.response()).thenReturn(response);
    var metadata = mock(McpOAuthMetadata.class);
    String challenge =
        "Bearer resource_metadata=\"https://data.example.test/.well-known/oauth-protected-resource\"";
    when(metadata.isMcpRequest(context)).thenReturn(true);
    when(metadata.challenge(context)).thenReturn(Optional.of(challenge));
    var mechanism =
        new HeaderBasedAuthenticationMechanism(authConfig(true, true), instance(), metadata);

    assertThatThrownBy(() -> mechanism.authenticate(context, manager).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class);
    verify(response).putHeader(HttpHeaders.WWW_AUTHENTICATE, challenge);
  }

  @Test
  void bearerIssuedForAnotherResourceIsRejectedWithTheChallenge() {
    var manager = mock(IdentityProviderManager.class);
    var audience = Set.of("https://db-two.apps.example.test/v1/mcp");
    var identity = jwtIdentity(audience);
    when(manager.authenticate(any())).thenReturn(Uni.createFrom().item(identity));
    var context = context(Map.of("Authorization", "Bearer encoded-jwt"));
    var response = mock(HttpServerResponse.class);
    when(context.response()).thenReturn(response);
    var metadata = mock(McpOAuthMetadata.class);
    String challenge =
        "Bearer resource_metadata=\"https://db-one.apps.example.test/.well-known/oauth-protected-resource\"";
    when(metadata.isMcpRequest(context)).thenReturn(true);
    when(metadata.challenge(context)).thenReturn(Optional.of(challenge));
    when(metadata.acceptsAudience(context, audience)).thenReturn(false);
    var mechanism =
        new HeaderBasedAuthenticationMechanism(authConfig(true, true), instance(), metadata);

    assertThatThrownBy(() -> mechanism.authenticate(context, manager).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class);
    verify(metadata).acceptsAudience(context, audience);
    verify(response).putHeader(HttpHeaders.WWW_AUTHENTICATE, challenge);
  }

  @Test
  void bearerIssuedForTheRequestedResourceAuthenticates() {
    var manager = mock(IdentityProviderManager.class);
    var audience = Set.of("https://db-one.apps.example.test/v1/mcp");
    var identity = jwtIdentity(audience);
    when(manager.authenticate(any())).thenReturn(Uni.createFrom().item(identity));
    var context = context(Map.of("Authorization", "Bearer encoded-jwt"));
    var metadata = mock(McpOAuthMetadata.class);
    when(metadata.isMcpRequest(context)).thenReturn(true);
    when(metadata.acceptsAudience(context, audience)).thenReturn(true);
    var mechanism =
        new HeaderBasedAuthenticationMechanism(authConfig(true, true), instance(), metadata);

    assertThat(mechanism.authenticate(context, manager).await().indefinitely()).isSameAs(identity);
  }

  @Test
  void legacyCredentialsSkipTheBearerAudienceCheck() {
    var manager = mock(IdentityProviderManager.class);
    var identity = mock(SecurityIdentity.class);
    when(manager.authenticate(any())).thenReturn(Uni.createFrom().item(identity));
    var metadata = mock(McpOAuthMetadata.class);
    var mechanism =
        new HeaderBasedAuthenticationMechanism(authConfig(true, true), instance(), metadata);

    assertThat(
            mechanism
                .authenticate(context(Map.of("Token", "legacy-token")), manager)
                .await()
                .indefinitely())
        .isSameAs(identity);
    verify(metadata, never()).acceptsAudience(any(), any());
  }

  @Test
  void disabledBearerAuthenticationPreservesLegacyHeaderPrecedence() {
    var manager = mock(IdentityProviderManager.class);
    var identity = mock(SecurityIdentity.class);
    when(manager.authenticate(any())).thenReturn(Uni.createFrom().item(identity));
    var context = context(Map.of("Authorization", "Bearer invalid-jwt", "Token", "legacy-token"));

    assertThat(mechanism(false, true).authenticate(context, manager).await().indefinitely())
        .isSameAs(identity);
    var request = ArgumentCaptor.forClass(AuthenticationRequest.class);
    verify(manager).authenticate(request.capture());
    assertThat(request.getValue()).isInstanceOf(HeaderAuthenticationRequest.class);
  }

  @Test
  void disabledBearerAuthenticationStopsTheNativeJwtMechanism() {
    var manager = mock(IdentityProviderManager.class);
    var context = context(Map.of("Authorization", "Bearer encoded-jwt"));

    assertThatThrownBy(
            () -> mechanism(false, true).authenticate(context, manager).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class);
    verifyNoInteractions(manager);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Token", "token", "X-Cassandra-Token"})
  void enabledMcpOAuthRejectsLegacyCredentials(String header) {
    var manager = mock(IdentityProviderManager.class);
    var context = context(Map.of(header, "legacy-token"));

    assertThatThrownBy(
            () -> mechanism(true, true).authenticate(context, manager).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class);
    verifyNoInteractions(manager);
  }

  @Test
  void enabledMcpOAuthRequiresBearerCredentials() {
    var manager = mock(IdentityProviderManager.class);

    assertThatThrownBy(
            () ->
                mechanism(true, true)
                    .authenticate(context(Map.of()), manager)
                    .await()
                    .indefinitely())
        .isInstanceOf(AuthenticationFailedException.class);
    verifyNoInteractions(manager);
  }

  @Test
  void enabledMcpOAuthPreservesLegacyHeaderPrecedenceOnRestPaths() {
    var manager = mock(IdentityProviderManager.class);
    var identity = mock(SecurityIdentity.class);
    when(manager.authenticate(any())).thenReturn(Uni.createFrom().item(identity));
    var context = context(Map.of("Authorization", "Bearer synthetic-jwt", "Token", "legacy-token"));

    assertThat(mechanism(true, true, false).authenticate(context, manager).await().indefinitely())
        .isSameAs(identity);
    var request = ArgumentCaptor.forClass(AuthenticationRequest.class);
    verify(manager).authenticate(request.capture());
    assertThat(request.getValue()).isInstanceOf(HeaderAuthenticationRequest.class);
  }

  @Test
  void enabledMcpOAuthDoesNotEnableBearerAuthenticationOnRestPaths() {
    var manager = mock(IdentityProviderManager.class);
    var context = context(Map.of("Authorization", "Bearer encoded-jwt"));

    assertThatThrownBy(
            () ->
                mechanism(true, true, false).authenticate(context, manager).await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class);
    verifyNoInteractions(manager);
  }

  @Test
  void disabledLegacyHeadersDoNotAuthenticate() {
    var manager = mock(IdentityProviderManager.class);

    assertThat(
            mechanism(true, false, false)
                .authenticate(context(Map.of("Token", "legacy-token")), manager)
                .await()
                .indefinitely())
        .isNull();
    verifyNoInteractions(manager);
  }

  @Test
  void coordinatesTheChallengeAheadOfTheNativeJwtMechanism() {
    var mechanism = mechanism(true, true);
    assertThat(mechanism.getPriority()).isGreaterThan(1000);
    assertThat(mechanism.getCredentialTypes())
        .contains(TokenAuthenticationRequest.class, HeaderAuthenticationRequest.class);
  }

  @Test
  void challengeUsesTheConfiguredMetadata() {
    var config = authConfig(true, true);
    var metadata = mock(McpOAuthMetadata.class);
    var context = context(Map.of());
    var challenge =
        "Bearer resource_metadata=\"https://data.example.test/.well-known/oauth-protected-resource\"";
    when(metadata.challenge(context)).thenReturn(Optional.of(challenge));
    var mechanism = new HeaderBasedAuthenticationMechanism(config, instance(), metadata);

    var result = mechanism.getChallenge(context).await().indefinitely();
    assertThat(result.status).isEqualTo(401);
    assertThat(result.getHeaders()).containsEntry(HttpHeaders.WWW_AUTHENTICATE, challenge);
  }

  private static HeaderBasedAuthenticationMechanism mechanism(boolean bearer, boolean legacy) {
    return mechanism(bearer, legacy, true);
  }

  private static HeaderBasedAuthenticationMechanism mechanism(
      boolean bearer, boolean legacy, boolean mcpRequest) {
    var metadata = mock(McpOAuthMetadata.class);
    when(metadata.isMcpRequest(any())).thenReturn(mcpRequest);
    when(metadata.challenge(any())).thenReturn(Optional.empty());
    when(metadata.acceptsAudience(any(), any())).thenReturn(true);
    return new HeaderBasedAuthenticationMechanism(authConfig(bearer, legacy), instance(), metadata);
  }

  private static AuthConfig authConfig(boolean bearer, boolean legacy) {
    var config = mock(AuthConfig.class);
    var headerConfig = mock(AuthConfig.HeaderBasedAuthConfig.class);
    var bearerConfig = mock(AuthConfig.BearerAuthConfig.class);
    when(config.headerBased()).thenReturn(headerConfig);
    when(headerConfig.enabled()).thenReturn(legacy);
    when(headerConfig.headerName()).thenReturn("Token");
    when(config.bearer()).thenReturn(bearerConfig);
    when(bearerConfig.enabled()).thenReturn(bearer);
    return config;
  }

  private static RoutingContext context(Map<String, String> headers) {
    var request = mock(HttpServerRequest.class);
    when(request.getHeader(any(String.class))).thenAnswer(call -> headers.get(call.getArgument(0)));
    var context = mock(RoutingContext.class);
    when(context.request()).thenReturn(request);
    return context;
  }

  private static SecurityIdentity jwtIdentity(Set<String> audience) {
    var token = mock(JsonWebToken.class);
    when(token.getAudience()).thenReturn(audience);
    var identity = mock(SecurityIdentity.class);
    when(identity.getPrincipal()).thenReturn(token);
    return identity;
  }

  @SuppressWarnings("unchecked")
  private static Instance<ErrorChallengeSender> instance() {
    return mock(Instance.class);
  }
}
