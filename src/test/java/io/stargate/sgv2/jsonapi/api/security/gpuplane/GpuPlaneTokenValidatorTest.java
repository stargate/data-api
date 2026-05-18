/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.stargate.sgv2.jsonapi.api.security.gpuplane;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.config.GpuPlaneAuthConfig;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class GpuPlaneTokenValidatorTest {

  private static final String VALID_ASTRA_CS =
      "AstraCS:abcdefghijklmnopqrstuvwx:"
          + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
  private static final String VALID_JWT = "eyJhbGciOi.eyJzdWIiOi.signature_part";

  private AstraCurrentOrgClient currentOrgClient;
  private AstraTokenAuthorizeClient tokenAuthorizeClient;
  private GpuPlaneAuthConfig config;
  private GpuPlaneTokenValidator validator;

  @BeforeEach
  void setUp() {
    currentOrgClient = mock(AstraCurrentOrgClient.class);
    tokenAuthorizeClient = mock(AstraTokenAuthorizeClient.class);
    config = mock(GpuPlaneAuthConfig.class);
    when(config.insightsPlaneUsername()).thenReturn(Optional.of("user"));
    when(config.insightsPlanePassword()).thenReturn(Optional.of("pass"));
    validator = new GpuPlaneTokenValidator(config, currentOrgClient, tokenAuthorizeClient);
  }

  @Test
  void deniesNullAndEmptyToken() {
    assertThat(validator.validate(null, null).await().indefinitely().allowed()).isFalse();
    assertThat(validator.validate("", null).await().indefinitely().allowed()).isFalse();
    verify(currentOrgClient, never()).currentOrg(anyString());
    verify(tokenAuthorizeClient, never()).authorize(anyString(), anyString(), any());
  }

  @Test
  void deniesUnrecognizedFormatWithoutCallingUpstream() {
    GpuPlaneTokenValidator.Result result =
        validator.validate("not-a-real-token", null).await().indefinitely();
    assertThat(result.allowed()).isFalse();
    assertThat(result.denyReason()).contains("unrecognized");
    verify(currentOrgClient, never()).currentOrg(anyString());
    verify(tokenAuthorizeClient, never()).authorize(anyString(), anyString(), any());
  }

  @Test
  void allowsValidAstraCsTokenAndExtractsOrg() {
    when(currentOrgClient.currentOrg(anyString()))
        .thenReturn(
            Uni.createFrom().item(new AstraCurrentOrgClient.CurrentOrgResponse("org-123")));

    GpuPlaneTokenValidator.Result result =
        validator.validate(VALID_ASTRA_CS, null).await().indefinitely();

    assertThat(result.allowed()).isTrue();
    assertThat(result.orgId()).isEqualTo("org-123");
    assertThat(result.tokenType()).isEqualTo(GpuPlaneTokenValidator.TokenType.ASTRA_CS);
  }

  @Test
  void deniesAstraCsWhenUpstreamReturnsEmptyId() {
    when(currentOrgClient.currentOrg(anyString()))
        .thenReturn(Uni.createFrom().item(new AstraCurrentOrgClient.CurrentOrgResponse("")));

    GpuPlaneTokenValidator.Result result =
        validator.validate(VALID_ASTRA_CS, null).await().indefinitely();

    assertThat(result.allowed()).isFalse();
    assertThat(result.denyReason()).contains("no id");
  }

  @Test
  void deniesAstraCsWhenUpstreamErrors() {
    when(currentOrgClient.currentOrg(anyString()))
        .thenReturn(Uni.createFrom().failure(new RuntimeException("401 Unauthorized")));

    GpuPlaneTokenValidator.Result result =
        validator.validate(VALID_ASTRA_CS, null).await().indefinitely();

    assertThat(result.allowed()).isFalse();
  }

  @Test
  void forwardsBearerPrefixAsIsToCurrentOrg() {
    when(currentOrgClient.currentOrg(anyString()))
        .thenReturn(
            Uni.createFrom().item(new AstraCurrentOrgClient.CurrentOrgResponse("org-1")));

    validator.validate("Bearer " + VALID_ASTRA_CS, null).await().indefinitely();

    ArgumentCaptor<String> auth = ArgumentCaptor.forClass(String.class);
    verify(currentOrgClient).currentOrg(auth.capture());
    assertThat(auth.getValue()).isEqualTo("Bearer " + VALID_ASTRA_CS);
  }

  @Test
  void allowsJwtWhenUpstreamAllowsAndOrgPresent() {
    when(tokenAuthorizeClient.authorize(anyString(), anyString(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(new AstraTokenAuthorizeClient.AuthorizeResponse(true, "org-jwt")));

    GpuPlaneTokenValidator.Result result =
        validator.validate(VALID_JWT, "tenant-7").await().indefinitely();

    assertThat(result.allowed()).isTrue();
    assertThat(result.orgId()).isEqualTo("org-jwt");
    assertThat(result.tokenType()).isEqualTo(GpuPlaneTokenValidator.TokenType.JWT);
  }

  @Test
  void deniesJwtWhenAllowFalse() {
    when(tokenAuthorizeClient.authorize(anyString(), anyString(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(new AstraTokenAuthorizeClient.AuthorizeResponse(false, "org-jwt")));

    GpuPlaneTokenValidator.Result result =
        validator.validate(VALID_JWT, "tenant-7").await().indefinitely();

    assertThat(result.allowed()).isFalse();
    assertThat(result.denyReason()).contains("allow=false");
  }

  @Test
  void buildsTenantScopedDrnWhenTenantIdProvided() {
    when(tokenAuthorizeClient.authorize(anyString(), anyString(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(new AstraTokenAuthorizeClient.AuthorizeResponse(true, "org-1")));

    validator.validate(VALID_JWT, "tenant-7").await().indefinitely();

    ArgumentCaptor<AstraTokenAuthorizeClient.AuthorizeRequest> body =
        ArgumentCaptor.forClass(AstraTokenAuthorizeClient.AuthorizeRequest.class);
    verify(tokenAuthorizeClient).authorize(anyString(), eq("true"), body.capture());
    assertThat(body.getValue().resources()).containsExactly("drn:astra:org:*:db:tenant-7");
    assertThat(body.getValue().actions()).containsExactly("org-db-view");
    assertThat(body.getValue().token()).isEqualTo(VALID_JWT);
  }

  @Test
  void buildsWildcardDrnWhenTenantIdMissing() {
    when(tokenAuthorizeClient.authorize(anyString(), anyString(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(new AstraTokenAuthorizeClient.AuthorizeResponse(true, "org-1")));

    validator.validate(VALID_JWT, null).await().indefinitely();
    validator.validate(VALID_JWT, "  ").await().indefinitely();

    ArgumentCaptor<AstraTokenAuthorizeClient.AuthorizeRequest> body =
        ArgumentCaptor.forClass(AstraTokenAuthorizeClient.AuthorizeRequest.class);
    verify(tokenAuthorizeClient, times(2)).authorize(anyString(), anyString(), body.capture());
    assertThat(body.getAllValues())
        .allSatisfy(b -> assertThat(b.resources()).containsExactly("drn:astra:org:*:db:*"));
  }

  @Test
  void deniesJwtWhenInsightsPlaneCredsMissing() {
    when(config.insightsPlaneUsername()).thenReturn(Optional.empty());

    GpuPlaneTokenValidator.Result result =
        validator.validate(VALID_JWT, "tenant-1").await().indefinitely();

    assertThat(result.allowed()).isFalse();
    assertThat(result.denyReason()).contains("credentials");
    verify(tokenAuthorizeClient, never()).authorize(anyString(), anyString(), any());
  }

  @Test
  void stripsBearerPrefixBeforeJwtBodyButRoutesByStrippedFormat() {
    when(tokenAuthorizeClient.authorize(anyString(), anyString(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(new AstraTokenAuthorizeClient.AuthorizeResponse(true, "org-1")));

    validator.validate("Bearer " + VALID_JWT, "tenant-1").await().indefinitely();

    ArgumentCaptor<AstraTokenAuthorizeClient.AuthorizeRequest> body =
        ArgumentCaptor.forClass(AstraTokenAuthorizeClient.AuthorizeRequest.class);
    verify(tokenAuthorizeClient).authorize(anyString(), anyString(), body.capture());
    assertThat(body.getValue().token()).isEqualTo(VALID_JWT);
  }
}
