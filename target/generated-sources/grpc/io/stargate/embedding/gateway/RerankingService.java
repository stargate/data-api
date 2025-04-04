package io.stargate.embedding.gateway;

import io.quarkus.grpc.MutinyService;

@jakarta.annotation.Generated(value = "by Mutiny Grpc generator", comments = "Source: embedding_gateway.proto")
public interface RerankingService extends MutinyService {

    io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request);

    io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request);
}
