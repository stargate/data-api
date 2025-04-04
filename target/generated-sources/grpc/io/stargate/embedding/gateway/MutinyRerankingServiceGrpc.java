package io.stargate.embedding.gateway;

import static io.stargate.embedding.gateway.RerankingServiceGrpc.getServiceDescriptor;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

@jakarta.annotation.Generated(value = "by Mutiny Grpc generator", comments = "Source: embedding_gateway.proto")
public final class MutinyRerankingServiceGrpc implements io.quarkus.grpc.MutinyGrpc {

    private MutinyRerankingServiceGrpc() {
    }

    public static MutinyRerankingServiceStub newMutinyStub(io.grpc.Channel channel) {
        return new MutinyRerankingServiceStub(channel);
    }

    /**
     * <pre>
     *  The embedding gateway gPRC API to reranking
     * </pre>
     */
    public static class MutinyRerankingServiceStub extends io.grpc.stub.AbstractStub<MutinyRerankingServiceStub> implements io.quarkus.grpc.MutinyStub {

        private RerankingServiceGrpc.RerankingServiceStub delegateStub;

        private MutinyRerankingServiceStub(io.grpc.Channel channel) {
            super(channel);
            delegateStub = RerankingServiceGrpc.newStub(channel);
        }

        private MutinyRerankingServiceStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
            delegateStub = RerankingServiceGrpc.newStub(channel).build(channel, callOptions);
        }

        @Override
        protected MutinyRerankingServiceStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new MutinyRerankingServiceStub(channel, callOptions);
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request) {
            return io.quarkus.grpc.stubs.ClientCalls.oneToOne(request, delegateStub::rerank);
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request) {
            return io.quarkus.grpc.stubs.ClientCalls.oneToOne(request, delegateStub::getSupportedRerankingProviders);
        }
    }

    /**
     * <pre>
     *  The embedding gateway gPRC API to reranking
     * </pre>
     */
    public static abstract class RerankingServiceImplBase implements io.grpc.BindableService {

        private String compression;

        /**
         * Set whether the server will try to use a compressed response.
         *
         * @param compression the compression, e.g {@code gzip}
         */
        public RerankingServiceImplBase withCompression(String compression) {
            this.compression = compression;
            return this;
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor()).addMethod(io.stargate.embedding.gateway.RerankingServiceGrpc.getRerankMethod(), asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest, io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse>(this, METHODID_RERANK, compression))).addMethod(io.stargate.embedding.gateway.RerankingServiceGrpc.getGetSupportedRerankingProvidersMethod(), asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse>(this, METHODID_GET_SUPPORTED_RERANKING_PROVIDERS, compression))).build();
        }
    }

    private static final int METHODID_RERANK = 0;

    private static final int METHODID_GET_SUPPORTED_RERANKING_PROVIDERS = 1;

    private static final class MethodHandlers<Req, Resp> implements io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>, io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>, io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>, io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {

        private final RerankingServiceImplBase serviceImpl;

        private final int methodId;

        private final String compression;

        MethodHandlers(RerankingServiceImplBase serviceImpl, int methodId, String compression) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
            this.compression = compression;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch(methodId) {
                case METHODID_RERANK:
                    io.quarkus.grpc.stubs.ServerCalls.oneToOne((io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse>) responseObserver, compression, serviceImpl::rerank);
                    break;
                case METHODID_GET_SUPPORTED_RERANKING_PROVIDERS:
                    io.quarkus.grpc.stubs.ServerCalls.oneToOne((io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse>) responseObserver, compression, serviceImpl::getSupportedRerankingProviders);
                    break;
                default:
                    throw new java.lang.AssertionError();
            }
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public io.grpc.stub.StreamObserver<Req> invoke(io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch(methodId) {
                default:
                    throw new java.lang.AssertionError();
            }
        }
    }
}
