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
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * MicroProfile REST client for the Astra {@code /v2/currentOrg} endpoint.
 *
 * <p>Used to resolve the calling org for legacy {@code AstraCS:...} tokens. The endpoint returns
 * 200 + {@code {id: ...}} when the token is valid, and a non-2xx status otherwise.
 *
 * <p>The base URL is supplied via {@code quarkus.rest-client.gpu-plane-current-org.url}.
 */
@RegisterRestClient(configKey = "gpu-plane-current-org")
public interface AstraCurrentOrgClient {

  @GET
  @Path("/v2/currentOrg")
  Uni<CurrentOrgResponse> currentOrg(@HeaderParam("Authorization") String authorization);

  @JsonIgnoreProperties(ignoreUnknown = true)
  record CurrentOrgResponse(String id) {}
}
