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

package io.stargate.sgv2.jsonapi.api.request.token;

import io.vertx.ext.web.RoutingContext;
import jakarta.ws.rs.core.SecurityContext;

/**
 * A {@link RequestAuthTokenResolver} that accepts Bearer JWTs issued by auth-manager.
 *
 * <p>SmallRye JWT (activated via {@code quarkus-smallrye-jwt}) validates the signature against the
 * configured JWKS endpoint ({@code stargate.auth.token-resolver.bearer.jwks-url}) and populates the
 * Vert.x {@link SecurityContext} principal with the verified claims. This resolver then extracts
 * the raw JWT string from the {@code Authorization: Bearer <token>} header and returns it so
 * downstream Cassandra/DSE calls can use it as the auth token.
 *
 * <p>Configuration (application.properties / env):
 *
 * <pre>
 * stargate.auth.token-resolver.type=bearer
 * stargate.auth.token-resolver.bearer.jwks-url=http://auth-manager:8080/v2/jwks
 * # SmallRye JWT will read the JWKS from this URL for signature verification:
 * mp.jwt.verify.publickey.location=${stargate.auth.token-resolver.bearer.jwks-url}
 * mp.jwt.verify.issuer=auth-manager
 * </pre>
 */
public class BearerJwtTokenResolver implements RequestAuthTokenResolver {

  private static final String BEARER_PREFIX = "Bearer ";

  /** {@inheritDoc} */
  @Override
  public String resolve(RoutingContext context, SecurityContext securityContext) {
    String authHeader = context.request().getHeader("Authorization");
    if (authHeader == null || !authHeader.startsWith(BEARER_PREFIX)) {
      return null;
    }
    return authHeader.substring(BEARER_PREFIX.length()).trim();
  }
}
