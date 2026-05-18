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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.config.GpuPlaneAuthConfig;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GpuPlaneAuthFilterTest {

  private GpuPlaneAuthConfig config;
  private GpuPlaneTokenValidator validator;
  private GpuPlaneAuthFilter filter;

  @BeforeEach
  void setUp() {
    config = mock(GpuPlaneAuthConfig.class);
    validator = mock(GpuPlaneTokenValidator.class);
    filter = new GpuPlaneAuthFilter(config, validator);
  }

  @Test
  void filterIsNoOpWhenFlagDisabled() {
    when(config.enabled()).thenReturn(false);
    ContainerRequestContext ctx = stubRequest("AstraCS:whatever", null, "spoofed-org");

    Uni<Response> outcome = filter.filter(ctx);
    Response result = outcome.await().indefinitely();

    assertThat(result).isNull();
    // When disabled the filter must not touch headers either.
    assertThat(ctx.getHeaders().getFirst(GpuPlaneAuthFilter.SPOOFABLE_ORG_HEADER))
        .isEqualTo("spoofed-org");
    verify(validator, never()).validate(anyString(), any());
  }

  @Test
  void rejectsMissingAuthorizationHeaderWith401() {
    when(config.enabled()).thenReturn(true);
    ContainerRequestContext ctx = stubRequest(null, null, null);

    Response result = filter.filter(ctx).await().indefinitely();

    assertThat(result.getStatus()).isEqualTo(401);
    verify(validator, never()).validate(anyString(), any());
  }

  @Test
  void rejectsWhenValidatorDenies() {
    when(config.enabled()).thenReturn(true);
    when(validator.validate(anyString(), any()))
        .thenReturn(Uni.createFrom().item(GpuPlaneTokenValidator.Result.deny("bad")));
    ContainerRequestContext ctx = stubRequest("AstraCS:bad", null, null);

    Response result = filter.filter(ctx).await().indefinitely();

    assertThat(result.getStatus()).isEqualTo(401);
  }

  @Test
  void allowsWhenValidatorAllows() {
    when(config.enabled()).thenReturn(true);
    when(validator.validate(anyString(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(
                    GpuPlaneTokenValidator.Result.allow(
                        "org-1", GpuPlaneTokenValidator.TokenType.JWT)));
    ContainerRequestContext ctx = stubRequest("Bearer some.jwt.token", "tenant-1", null);

    Response result = filter.filter(ctx).await().indefinitely();

    assertThat(result).isNull();
  }

  @Test
  void stripsSpoofableOrgHeaderEvenOnSuccess() {
    when(config.enabled()).thenReturn(true);
    when(validator.validate(anyString(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(
                    GpuPlaneTokenValidator.Result.allow(
                        "org-1", GpuPlaneTokenValidator.TokenType.ASTRA_CS)));
    ContainerRequestContext ctx = stubRequest("AstraCS:something", null, "spoofed-org");

    filter.filter(ctx).await().indefinitely();

    assertThat(ctx.getHeaders().containsKey(GpuPlaneAuthFilter.SPOOFABLE_ORG_HEADER)).isFalse();
  }

  @Test
  void readsLegacyLowercaseTenantHeader() {
    when(config.enabled()).thenReturn(true);
    when(validator.validate(anyString(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(
                    GpuPlaneTokenValidator.Result.allow(
                        "org-1", GpuPlaneTokenValidator.TokenType.JWT)));

    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    headers.putSingle(HttpHeaders.AUTHORIZATION, "Bearer x.y.z");
    headers.putSingle(GpuPlaneAuthFilter.LEGACY_TENANT_HEADER, "tenant-legacy");
    ContainerRequestContext ctx = stubFromHeaders(headers);

    filter.filter(ctx).await().indefinitely();

    verify(validator).validate("Bearer x.y.z", "tenant-legacy");
  }

  private static ContainerRequestContext stubRequest(
      String authHeader, String tenantHeader, String spoofedOrgHeader) {
    MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
    if (authHeader != null) {
      headers.putSingle(HttpHeaders.AUTHORIZATION, authHeader);
    }
    if (tenantHeader != null) {
      headers.putSingle(GpuPlaneAuthFilter.TENANT_HEADER, tenantHeader);
    }
    if (spoofedOrgHeader != null) {
      headers.putSingle(GpuPlaneAuthFilter.SPOOFABLE_ORG_HEADER, spoofedOrgHeader);
    }
    return stubFromHeaders(headers);
  }

  private static ContainerRequestContext stubFromHeaders(MultivaluedMap<String, String> headers) {
    ContainerRequestContext ctx = mock(ContainerRequestContext.class);
    when(ctx.getHeaders()).thenReturn(headers);
    when(ctx.getHeaderString(anyString()))
        .thenAnswer(inv -> headers.getFirst(inv.getArgument(0, String.class)));
    return ctx;
  }
}
