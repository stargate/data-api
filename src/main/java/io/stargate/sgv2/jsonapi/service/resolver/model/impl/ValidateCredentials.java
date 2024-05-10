package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.quarkus.grpc.GrpcClient;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingServiceGrpc;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ValidateCredentials {
  @GrpcClient("embedding")
  EmbeddingServiceGrpc.EmbeddingServiceBlockingStub embeddingService;

  @Inject DataApiRequestInfo dataApiRequestInfo;

  public boolean validate(String provider, String value) {
    EmbeddingGateway.ValidateCredentialRequest.Builder validateCredentialRequest =
        EmbeddingGateway.ValidateCredentialRequest.newBuilder();
    validateCredentialRequest.setCredential(value);
    validateCredentialRequest.setTenantId(dataApiRequestInfo.getTenantId().orElse("default"));
    validateCredentialRequest.setProviderName(provider);
    validateCredentialRequest.setToken(dataApiRequestInfo.getCassandraToken().orElse(""));

    final EmbeddingGateway.ValidateCredentialResponse validateCredentialResponse =
        embeddingService.validateCredential(validateCredentialRequest.build());
    if (validateCredentialResponse.hasError()) {
      throw ErrorCode.VECTORIZE_CREDENTIAL_INVALID.toApiException(
          " with error: %s", validateCredentialResponse.getError().getErrorMessage());
    }
    return validateCredentialResponse.getValidity();
  }
}
