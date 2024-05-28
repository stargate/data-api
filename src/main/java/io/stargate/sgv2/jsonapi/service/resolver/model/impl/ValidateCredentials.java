package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.runtime.ShutdownEvent;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingServiceGrpc;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ValidateCredentials {

  private @ConfigProperty(name = "quarkus.grpc.clients.\"embedding\".host") String host;

  @ConfigProperty(name = "quarkus.grpc.clients.\"embedding\".port")
  private int port;

  private ManagedChannel channel = null;

  @Inject DataApiRequestInfo dataApiRequestInfo;

  public boolean validate(String provider, String value) {
    if (channel == null) {
      channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }
    EmbeddingGateway.ValidateCredentialRequest.Builder validateCredentialRequest =
        EmbeddingGateway.ValidateCredentialRequest.newBuilder();
    validateCredentialRequest.setCredential(value);
    validateCredentialRequest.setTenantId(dataApiRequestInfo.getTenantId().orElse("default"));
    validateCredentialRequest.setProviderName(provider);
    validateCredentialRequest.setToken(dataApiRequestInfo.getCassandraToken().orElse(""));
    EmbeddingServiceGrpc.EmbeddingServiceBlockingStub embeddingService =
        EmbeddingServiceGrpc.newBlockingStub(channel);
    final EmbeddingGateway.ValidateCredentialResponse validateCredentialResponse =
        embeddingService.validateCredential(validateCredentialRequest.build());
    if (validateCredentialResponse.hasError()) {
      throw ErrorCode.VECTORIZE_CREDENTIAL_INVALID.toApiException(
          " with error: %s", validateCredentialResponse.getError().getErrorMessage());
    }
    return validateCredentialResponse.getValidity();
  }

  void onStop(@Observes ShutdownEvent ev) {
    if (channel != null) channel.shutdown();
  }
}
