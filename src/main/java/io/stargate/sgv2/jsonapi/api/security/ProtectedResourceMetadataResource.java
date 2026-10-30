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
 */
package io.stargate.sgv2.jsonapi.api.security;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Exposes RFC 9728 Protected Resource Metadata at {@code /.well-known/oauth-protected-resource}.
 *
 * <p>MCP clients fetch this document to discover which authorization server to authenticate
 * against. This endpoint is unauthenticated (permit-all in Quarkus HTTP security config).
 */
@Path("/.well-known/oauth-protected-resource")
public class ProtectedResourceMetadataResource {

  @ConfigProperty(name = "stargate.mcp.auth.server-url", defaultValue = "https://auth.astra.datastax.com")
  String authServerUrl;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Object> metadata(@Context UriInfo uriInfo) {
    URI baseUri = uriInfo.getBaseUri();
    String resourceUrl = baseUri.getScheme() + "://" + baseUri.getAuthority() + "/v1/mcp";
    return Map.of(
        "resource", resourceUrl,
        "authorization_servers", List.of(authServerUrl),
        "bearer_methods_supported", List.of("header"),
        "scopes_supported", List.of("astra:db:read", "astra:db:write", "astra:db:admin"));
  }
}
