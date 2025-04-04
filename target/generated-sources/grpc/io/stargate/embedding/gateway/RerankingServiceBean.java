package io.stargate.embedding.gateway;

import io.grpc.BindableService;
import io.quarkus.grpc.GrpcService;
import io.quarkus.grpc.MutinyBean;

@jakarta.annotation.Generated(value = "by Mutiny Grpc generator", comments = "Source: embedding_gateway.proto")
public class RerankingServiceBean extends MutinyRerankingServiceGrpc.RerankingServiceImplBase implements BindableService, MutinyBean {

    private final RerankingService delegate;

    RerankingServiceBean(@GrpcService RerankingService delegate) {
        this.delegate = delegate;
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request) {
        try {
            return delegate.rerank(request);
        } catch (UnsupportedOperationException e) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request) {
        try {
            return delegate.getSupportedRerankingProviders(request);
        } catch (UnsupportedOperationException e) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }
    }
}
