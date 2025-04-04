package io.stargate.embedding.gateway;

import io.quarkus.grpc.MutinyService;

@jakarta.annotation.Generated(value = "by Mutiny Grpc generator", comments = "Source: embedding_gateway.proto")
public interface EmbeddingService extends MutinyService {

    io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request);

    io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request);

    io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request);
}
