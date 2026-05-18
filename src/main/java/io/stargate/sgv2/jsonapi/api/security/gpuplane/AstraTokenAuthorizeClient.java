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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import java.util.List;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * MicroProfile REST client for the Astra {@code /v2/token/authorize} endpoint.
 *
 * <p>Used to validate JWT-style tokens. Requires Insights-Plane basic-auth credentials and the
 * {@code X-DataStax-Insights-Plane: true} header. The endpoint returns 200 with {@code allow} +
 * {@code org_id} for valid tokens (regardless of authorization outcome), and 4xx for malformed
 * input.
 *
 * <p>The base URL is supplied via {@code quarkus.rest-client.gpu-plane-token-authorize.url}.
 */
@RegisterRestClient(configKey = "gpu-plane-token-authorize")
public interface AstraTokenAuthorizeClient {

  @POST
  @Path("/v2/token/authorize")
  Uni<AuthorizeResponse> authorize(
      @HeaderParam("Authorization") String basicAuth,
      @HeaderParam("X-DataStax-Insights-Plane") String insightsPlaneFlag,
      AuthorizeRequest body);

  record AuthorizeRequest(String token, List<String> actions, List<String> resources) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  record AuthorizeResponse(boolean allow, @JsonProperty("org_id") String orgId) {}
}
