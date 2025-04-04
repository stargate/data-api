package io.stargate.embedding.gateway;

import io.grpc.BindableService;
import io.quarkus.grpc.GrpcService;
import io.quarkus.grpc.MutinyBean;

@jakarta.annotation.Generated(value = "by Mutiny Grpc generator", comments = "Source: embedding_gateway.proto")
public class EmbeddingServiceBean extends MutinyEmbeddingServiceGrpc.EmbeddingServiceImplBase implements BindableService, MutinyBean {

    private final EmbeddingService delegate;

    EmbeddingServiceBean(@GrpcService EmbeddingService delegate) {
        this.delegate = delegate;
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request) {
        try {
            return delegate.embed(request);
        } catch (UnsupportedOperationException e) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request) {
        try {
            return delegate.getSupportedProviders(request);
        } catch (UnsupportedOperationException e) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request) {
        try {
            return delegate.validateCredential(request);
        } catch (UnsupportedOperationException e) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }
    }
}
