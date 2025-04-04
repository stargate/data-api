package io.stargate.embedding.gateway;

import static io.stargate.embedding.gateway.EmbeddingServiceGrpc.getServiceDescriptor;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;

@jakarta.annotation.Generated(value = "by Mutiny Grpc generator", comments = "Source: embedding_gateway.proto")
public final class MutinyEmbeddingServiceGrpc implements io.quarkus.grpc.MutinyGrpc {

    private MutinyEmbeddingServiceGrpc() {
    }

    public static MutinyEmbeddingServiceStub newMutinyStub(io.grpc.Channel channel) {
        return new MutinyEmbeddingServiceStub(channel);
    }

    /**
     * <pre>
     *  The embedding gateway gPRC API to generate embeddings
     * </pre>
     */
    public static class MutinyEmbeddingServiceStub extends io.grpc.stub.AbstractStub<MutinyEmbeddingServiceStub> implements io.quarkus.grpc.MutinyStub {

        private EmbeddingServiceGrpc.EmbeddingServiceStub delegateStub;

        private MutinyEmbeddingServiceStub(io.grpc.Channel channel) {
            super(channel);
            delegateStub = EmbeddingServiceGrpc.newStub(channel);
        }

        private MutinyEmbeddingServiceStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
            delegateStub = EmbeddingServiceGrpc.newStub(channel).build(channel, callOptions);
        }

        @Override
        protected MutinyEmbeddingServiceStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new MutinyEmbeddingServiceStub(channel, callOptions);
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request) {
            return io.quarkus.grpc.stubs.ClientCalls.oneToOne(request, delegateStub::embed);
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request) {
            return io.quarkus.grpc.stubs.ClientCalls.oneToOne(request, delegateStub::getSupportedProviders);
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request) {
            return io.quarkus.grpc.stubs.ClientCalls.oneToOne(request, delegateStub::validateCredential);
        }
    }

    /**
     * <pre>
     *  The embedding gateway gPRC API to generate embeddings
     * </pre>
     */
    public static abstract class EmbeddingServiceImplBase implements io.grpc.BindableService {

        private String compression;

        /**
         * Set whether the server will try to use a compressed response.
         *
         * @param compression the compression, e.g {@code gzip}
         */
        public EmbeddingServiceImplBase withCompression(String compression) {
            this.compression = compression;
            return this;
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }

        public io.smallrye.mutiny.Uni<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request) {
            throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNIMPLEMENTED);
        }

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor()).addMethod(io.stargate.embedding.gateway.EmbeddingServiceGrpc.getEmbedMethod(), asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest, io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse>(this, METHODID_EMBED, compression))).addMethod(io.stargate.embedding.gateway.EmbeddingServiceGrpc.getGetSupportedProvidersMethod(), asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse>(this, METHODID_GET_SUPPORTED_PROVIDERS, compression))).addMethod(io.stargate.embedding.gateway.EmbeddingServiceGrpc.getValidateCredentialMethod(), asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest, io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse>(this, METHODID_VALIDATE_CREDENTIAL, compression))).build();
        }
    }

    private static final int METHODID_EMBED = 0;

    private static final int METHODID_GET_SUPPORTED_PROVIDERS = 1;

    private static final int METHODID_VALIDATE_CREDENTIAL = 2;

    private static final class MethodHandlers<Req, Resp> implements io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>, io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>, io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>, io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {

        private final EmbeddingServiceImplBase serviceImpl;

        private final int methodId;

        private final String compression;

        MethodHandlers(EmbeddingServiceImplBase serviceImpl, int methodId, String compression) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
            this.compression = compression;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch(methodId) {
                case METHODID_EMBED:
                    io.quarkus.grpc.stubs.ServerCalls.oneToOne((io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse>) responseObserver, compression, serviceImpl::embed);
                    break;
                case METHODID_GET_SUPPORTED_PROVIDERS:
                    io.quarkus.grpc.stubs.ServerCalls.oneToOne((io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse>) responseObserver, compression, serviceImpl::getSupportedProviders);
                    break;
                case METHODID_VALIDATE_CREDENTIAL:
                    io.quarkus.grpc.stubs.ServerCalls.oneToOne((io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse>) responseObserver, compression, serviceImpl::validateCredential);
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
