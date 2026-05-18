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

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.config.GpuPlaneAuthConfig;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.jboss.resteasy.reactive.server.ServerRequestFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request filter that pre-validates the inbound {@code Authorization} header against Astra
 * control-plane endpoints, gated by {@code stargate.jsonapi.gpu-plane-auth.enabled}.
 *
 * <p>Disabled by default — this filter is a no-op (does not even read the validator) unless the
 * GPU-plane flag is on, so it is safe to leave merged in the OSS distribution. When enabled, the
 * filter:
 *
 * <ol>
 *   <li>Strips any inbound {@code X-Datastax-Org} header to prevent spoofing — that header used to
 *       be injected authoritatively by the {@code gpu-api-gateway} authorizer Lambda.
 *   <li>Reads the {@code Authorization} (or {@code Token}) header and the {@code Tenant-Id} header.
 *   <li>Calls {@link GpuPlaneTokenValidator} (Caffeine-cached) — rejects the request with HTTP 401
 *       if the token is missing, malformed, or rejected upstream.
 * </ol>
 *
 * <p>This runs in addition to the existing {@code HeaderBasedAuthenticationMechanism}; that
 * mechanism continues to pass the token to Cassandra as today. The new filter only adds an
 * up-front rejection layer.
 */
@Singleton
public class GpuPlaneAuthFilter {

  private static final Logger logger = LoggerFactory.getLogger(GpuPlaneAuthFilter.class);

  static final String SPOOFABLE_ORG_HEADER = "X-Datastax-Org";
  static final String TENANT_HEADER = "Tenant-Id";
  static final String LEGACY_TENANT_HEADER = "tenant-id";

  private final GpuPlaneAuthConfig config;
  private final GpuPlaneTokenValidator validator;

  @Inject
  public GpuPlaneAuthFilter(GpuPlaneAuthConfig config, GpuPlaneTokenValidator validator) {
    this.config = config;
    this.validator = validator;
  }

  @ServerRequestFilter
  public Uni<Response> filter(ContainerRequestContext requestContext) {
    if (!config.enabled()) {
      return Uni.createFrom().nullItem();
    }

    // Always strip the spoofable org header — even on the success path, downstream handlers
    // must derive the org from the validated token, not from an inbound header.
    requestContext.getHeaders().remove(SPOOFABLE_ORG_HEADER);

    String token = readAuthHeader(requestContext);
    if (token == null || token.isBlank()) {
      return Uni.createFrom().item(unauthorized("missing Authorization header"));
    }

    String tenantId = readTenantHeader(requestContext);

    return validator
        .validate(token, tenantId)
        .map(
            result -> {
              if (result.allowed()) {
                return null; // continue
              }
              if (logger.isDebugEnabled()) {
                logger.debug("GPU-plane auth denied: {}", result.denyReason());
              }
              return unauthorized("token validation failed");
            });
  }

  private static String readAuthHeader(ContainerRequestContext ctx) {
    String value = ctx.getHeaderString(HttpHeaders.AUTHORIZATION);
    if (value == null || value.isBlank()) {
      value = ctx.getHeaderString("Token");
    }
    if (value == null || value.isBlank()) {
      value = ctx.getHeaderString("token");
    }
    return value;
  }

  private static String readTenantHeader(ContainerRequestContext ctx) {
    String value = ctx.getHeaderString(TENANT_HEADER);
    if (value == null || value.isBlank()) {
      value = ctx.getHeaderString(LEGACY_TENANT_HEADER);
    }
    return value;
  }

  private static Response unauthorized(String message) {
    return Response.status(Response.Status.UNAUTHORIZED)
        .type(MediaType.TEXT_PLAIN_TYPE)
        .entity(message)
        .build();
  }
}
