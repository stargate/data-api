package io.stargate.embedding.gateway;

import java.util.function.BiFunction;
import io.quarkus.grpc.MutinyClient;

@jakarta.annotation.Generated(value = "by Mutiny Grpc generator", comments = "Source: embedding_gateway.proto")
public class EmbeddingServiceClient implements EmbeddingService, MutinyClient<MutinyEmbeddingServiceGrpc.MutinyEmbeddingServiceStub> {

    private final MutinyEmbeddingServiceGrpc.MutinyEmbeddingServiceStub stub;

    public EmbeddingServiceClient(String name, io.grpc.Channel channel, BiFunction<String, MutinyEmbeddingServiceGrpc.MutinyEmbeddingServiceStub, MutinyEmbeddingServiceGrpc.MutinyEmbeddingServiceStub> stubConfigurator) {
        this.stub = stubConfigurator.apply(name, MutinyEmbeddingServiceGrpc.newMutinyStub(channel));
    }

    private EmbeddingServiceClient(MutinyEmbeddingServiceGrpc.MutinyEmbeddingServiceStub stub) {
        this.stub = stub;
    }

    public EmbeddingServiceClient newInstanceWithStub(MutinyEmbeddingServiceGrpc.MutinyEmbeddingServiceStub stub) {
        return new EmbeddingServiceClient(stub);
    }

    @Override
    public MutinyEmbeddingServiceGrpc.MutinyEmbeddingServiceStub getStub() {
        return stub;
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request) {
        return stub.embed(request);
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request) {
        return stub.getSupportedProviders(request);
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request) {
        return stub.validateCredential(request);
    }
}
