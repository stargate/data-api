package io.stargate.embedding.gateway;

import java.util.function.BiFunction;
import io.quarkus.grpc.MutinyClient;

@jakarta.annotation.Generated(value = "by Mutiny Grpc generator", comments = "Source: embedding_gateway.proto")
public class RerankingServiceClient implements RerankingService, MutinyClient<MutinyRerankingServiceGrpc.MutinyRerankingServiceStub> {

    private final MutinyRerankingServiceGrpc.MutinyRerankingServiceStub stub;

    public RerankingServiceClient(String name, io.grpc.Channel channel, BiFunction<String, MutinyRerankingServiceGrpc.MutinyRerankingServiceStub, MutinyRerankingServiceGrpc.MutinyRerankingServiceStub> stubConfigurator) {
        this.stub = stubConfigurator.apply(name, MutinyRerankingServiceGrpc.newMutinyStub(channel));
    }

    private RerankingServiceClient(MutinyRerankingServiceGrpc.MutinyRerankingServiceStub stub) {
        this.stub = stub;
    }

    public RerankingServiceClient newInstanceWithStub(MutinyRerankingServiceGrpc.MutinyRerankingServiceStub stub) {
        return new RerankingServiceClient(stub);
    }

    @Override
    public MutinyRerankingServiceGrpc.MutinyRerankingServiceStub getStub() {
        return stub;
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request) {
        return stub.rerank(request);
    }

    @Override
    public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request) {
        return stub.getSupportedRerankingProviders(request);
    }
}
