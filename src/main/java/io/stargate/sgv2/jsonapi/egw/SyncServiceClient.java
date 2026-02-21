package io.stargate.sgv2.jsonapi.egw;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.cache.CacheResult;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.*;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class SyncServiceClient {

  @Inject SyncServiceOperationConfiguration syncServiceConfiguration;

  private static final Logger logger = LoggerFactory.getLogger(SyncServiceClient.class);

  @RestClient SyncService syncService;

  public Uni<EmbeddingGateway.ValidateCredentialResponse> validateKey(
      String token, String tenant, String provider, String key) {
    return syncService
        .valid("Bearer " + token, UUID.randomUUID().toString(), tenant, provider, key)
        .onFailure(
            error -> {
              return error instanceof APIException apiException
                  && Objects.equals(
                      apiException.code,
                      EmbeddingProviderException.Code
                          .EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE
                          .name());
            })
        .retry()
        .withBackOff(Duration.ofMillis(syncServiceConfiguration.retry().retryDelayMillis()))
        .atMost(syncServiceConfiguration.retry().maxRetries())
        .map(
            res -> {
              boolean validity = true;
              if (res.credentials() == null || res.credentials().isEmpty()) {
                validity = false;
              } else {
                for (Map.Entry<String, Boolean> entry : res.credentials().entrySet()) {
                  validity &= entry.getValue();
                }
              }
              final EmbeddingGateway.ValidateCredentialResponse.Builder responseBuilder =
                  EmbeddingGateway.ValidateCredentialResponse.newBuilder();
              responseBuilder.setValidity(validity);

              if (res.errors() != null && !res.errors().isEmpty()) {
                final EmbeddingGateway.ValidateCredentialResponse.Error.Builder errorBuilder =
                    EmbeddingGateway.ValidateCredentialResponse.Error.newBuilder();
                responseBuilder.setError(
                    errorBuilder
                        .setErrorCode(res.errors().get(0).errorId())
                        .setErrorTitle(res.errors().get(0).errorId())
                        .setErrorBody(res.errors().get(0).message())
                        .build());
              }
              return responseBuilder.build();
            });
  }

  @CacheResult(cacheName = "credentials")
  public Uni<Map<String, String>> getCredential(
      String token, Tenant tenant, String provider, String cred) {
    return syncService
        .credential(
            "Bearer " + token, UUID.randomUUID().toString(), tenant.toString(), provider, cred)
        .onFailure(
            error -> {
              return error instanceof APIException apiException
                  && Objects.equals(
                      apiException.code,
                      EmbeddingProviderException.Code
                          .EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE
                          .name());
            })
        .retry()
        .withBackOff(Duration.ofMillis(syncServiceConfiguration.retry().retryDelayMillis()))
        .atMost(syncServiceConfiguration.retry().maxRetries())
        // Handle the response
        .map(
            res -> {
              Map<String, String> credentials = res.credentials();
              if (res.errors() != null && !res.errors().isEmpty()) {
                logger.error(
                    String.format(
                        "Credential error from SyncService : (%s) %s ",
                        res.errors().get(0).errorId(), res.errors().get(0).message()));

                throw EmbeddingProviderException.Code
                    .EMBEDDING_GATEWAY_UNABLE_RESOLVE_AUTHENTICATION_TYPE
                    .get(
                        "errorMessage",
                        "Credential error from SyncService : (%s) %s "
                            .formatted(
                                res.errors().get(0).errorId(), res.errors().get(0).message()));
              }
              return credentials;
            });
  }

  @RegisterRestClient(configKey = "credentials")
  public interface SyncService {
    @GET
    @Path("/databases/{dbId}/integrations-type/{type}/creds/{name}/valid")
    Uni<ValidateResponse> valid(
        @HeaderParam("Authorization") String accessToken,
        @HeaderParam("X-Datastax-Request-ID") String requestId,
        @PathParam("dbId") String dbId,
        @PathParam("type") String type,
        @PathParam("name") String name);

    @GET
    @Path("/databases/{dbId}/integrations-type/{type}/creds/{name}")
    Uni<CredentialResponse> credential(
        @HeaderParam("Authorization") String accessToken,
        @HeaderParam("X-Datastax-Request-ID") String requestId,
        @PathParam("dbId") String dbid,
        @PathParam("type") String type,
        @PathParam("name") String name);

    @ClientExceptionMapper
    static RuntimeException mapException(Response response) {
      return SyncServiceErrorMessageMapper.getDefaultException(response);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record ValidateResponse(
      @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, Boolean> credentials,
      List<Error> errors) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Error(@JsonProperty("ID") String errorId, String message) {}
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record CredentialResponse(
      @JsonInclude(JsonInclude.Include.NON_NULL) Map<String, String> credentials,
      List<Error> errors) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Error(@JsonProperty("ID") String errorId, String message) {}
  }
}
