package io.stargate.sgv2.jsonapi.service.resolver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.quarkus.runtime.ShutdownEvent;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.embedding.gateway.EmbeddingServiceGrpc;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
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

  @Inject RequestContext requestContext;

  public boolean validate(String provider, String value) {

    if (channel == null) {
      channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    }

    EmbeddingGateway.ValidateCredentialRequest.Builder validateCredentialRequest =
        EmbeddingGateway.ValidateCredentialRequest.newBuilder();
    validateCredentialRequest.setCredential(value);
    validateCredentialRequest.setTenantId(requestContext.getTenant().toString());
    validateCredentialRequest.setProviderName(provider);
    validateCredentialRequest.setToken(requestContext.getAuthToken());
    EmbeddingServiceGrpc.EmbeddingServiceBlockingStub embeddingService =
        EmbeddingServiceGrpc.newBlockingStub(channel);
    final EmbeddingGateway.ValidateCredentialResponse validateCredentialResponse =
        embeddingService.validateCredential(validateCredentialRequest.build());

    if (validateCredentialResponse.hasError()) {
      throw ErrorCodeV1.VECTORIZE_CREDENTIAL_INVALID.toApiException(
          " with error: %s", validateCredentialResponse.getError().getErrorMessage());
    }
    return validateCredentialResponse.getValidity();
  }

  void onStop(@Observes ShutdownEvent ev) {
    if (channel != null) channel.shutdown();
  }
}
