package io.stargate.embedding.gateway;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * The embedding gateway gPRC API to reranking
 * </pre>
 */
@io.quarkus.Generated(value = "by gRPC proto compiler (version 1.65.1)", comments = "Source: embedding_gateway.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class RerankingServiceGrpc {

    private RerankingServiceGrpc() {
    }

    public static final java.lang.String SERVICE_NAME = "stargate.RerankingService";

    // Static method descriptors that strictly reflect the proto.
    private static volatile io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest, io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> getRerankMethod;

    @io.grpc.stub.annotations.RpcMethod(fullMethodName = SERVICE_NAME + '/' + "Rerank", requestType = io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest.class, responseType = io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse.class, methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest, io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> getRerankMethod() {
        io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest, io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> getRerankMethod;
        if ((getRerankMethod = RerankingServiceGrpc.getRerankMethod) == null) {
            synchronized (RerankingServiceGrpc.class) {
                if ((getRerankMethod = RerankingServiceGrpc.getRerankMethod) == null) {
                    RerankingServiceGrpc.getRerankMethod = getRerankMethod = io.grpc.MethodDescriptor.<io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest, io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse>newBuilder().setType(io.grpc.MethodDescriptor.MethodType.UNARY).setFullMethodName(generateFullMethodName(SERVICE_NAME, "Rerank")).setSampledToLocalTracing(true).setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest.getDefaultInstance())).setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse.getDefaultInstance())).setSchemaDescriptor(new RerankingServiceMethodDescriptorSupplier("Rerank")).build();
                }
            }
        }
        return getRerankMethod;
    }

    private static volatile io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getGetSupportedRerankingProvidersMethod;

    @io.grpc.stub.annotations.RpcMethod(fullMethodName = SERVICE_NAME + '/' + "GetSupportedRerankingProviders", requestType = io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest.class, responseType = io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse.class, methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getGetSupportedRerankingProvidersMethod() {
        io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getGetSupportedRerankingProvidersMethod;
        if ((getGetSupportedRerankingProvidersMethod = RerankingServiceGrpc.getGetSupportedRerankingProvidersMethod) == null) {
            synchronized (RerankingServiceGrpc.class) {
                if ((getGetSupportedRerankingProvidersMethod = RerankingServiceGrpc.getGetSupportedRerankingProvidersMethod) == null) {
                    RerankingServiceGrpc.getGetSupportedRerankingProvidersMethod = getGetSupportedRerankingProvidersMethod = io.grpc.MethodDescriptor.<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse>newBuilder().setType(io.grpc.MethodDescriptor.MethodType.UNARY).setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSupportedRerankingProviders")).setSampledToLocalTracing(true).setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest.getDefaultInstance())).setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse.getDefaultInstance())).setSchemaDescriptor(new RerankingServiceMethodDescriptorSupplier("GetSupportedRerankingProviders")).build();
                }
            }
        }
        return getGetSupportedRerankingProvidersMethod;
    }

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static RerankingServiceStub newStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<RerankingServiceStub> factory = new io.grpc.stub.AbstractStub.StubFactory<RerankingServiceStub>() {

            @java.lang.Override
            public RerankingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                return new RerankingServiceStub(channel, callOptions);
            }
        };
        return RerankingServiceStub.newStub(factory, channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static RerankingServiceBlockingStub newBlockingStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<RerankingServiceBlockingStub> factory = new io.grpc.stub.AbstractStub.StubFactory<RerankingServiceBlockingStub>() {

            @java.lang.Override
            public RerankingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                return new RerankingServiceBlockingStub(channel, callOptions);
            }
        };
        return RerankingServiceBlockingStub.newStub(factory, channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary calls on the service
     */
    public static RerankingServiceFutureStub newFutureStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<RerankingServiceFutureStub> factory = new io.grpc.stub.AbstractStub.StubFactory<RerankingServiceFutureStub>() {

            @java.lang.Override
            public RerankingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                return new RerankingServiceFutureStub(channel, callOptions);
            }
        };
        return RerankingServiceFutureStub.newStub(factory, channel);
    }

    /**
     * <pre>
     * The embedding gateway gPRC API to reranking
     * </pre>
     */
    public interface AsyncService {

        /**
         */
        default void rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRerankMethod(), responseObserver);
        }

        /**
         */
        default void getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSupportedRerankingProvidersMethod(), responseObserver);
        }
    }

    /**
     * Base class for the server implementation of the service RerankingService.
     * <pre>
     * The embedding gateway gPRC API to reranking
     * </pre>
     */
    public static abstract class RerankingServiceImplBase implements io.grpc.BindableService, AsyncService {

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return RerankingServiceGrpc.bindService(this);
        }
    }

    /**
     * A stub to allow clients to do asynchronous rpc calls to service RerankingService.
     * <pre>
     * The embedding gateway gPRC API to reranking
     * </pre>
     */
    public static class RerankingServiceStub extends io.grpc.stub.AbstractAsyncStub<RerankingServiceStub> {

        private RerankingServiceStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected RerankingServiceStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new RerankingServiceStub(channel, callOptions);
        }

        /**
         */
        public void rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(getChannel().newCall(getRerankMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(getChannel().newCall(getGetSupportedRerankingProvidersMethod(), getCallOptions()), request, responseObserver);
        }
    }

    /**
     * A stub to allow clients to do synchronous rpc calls to service RerankingService.
     * <pre>
     * The embedding gateway gPRC API to reranking
     * </pre>
     */
    public static class RerankingServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<RerankingServiceBlockingStub> {

        private RerankingServiceBlockingStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected RerankingServiceBlockingStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new RerankingServiceBlockingStub(channel, callOptions);
        }

        /**
         */
        public io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(getChannel(), getRerankMethod(), getCallOptions(), request);
        }

        /**
         */
        public io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(getChannel(), getGetSupportedRerankingProvidersMethod(), getCallOptions(), request);
        }
    }

    /**
     * A stub to allow clients to do ListenableFuture-style rpc calls to service RerankingService.
     * <pre>
     * The embedding gateway gPRC API to reranking
     * </pre>
     */
    public static class RerankingServiceFutureStub extends io.grpc.stub.AbstractFutureStub<RerankingServiceFutureStub> {

        private RerankingServiceFutureStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected RerankingServiceFutureStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new RerankingServiceFutureStub(channel, callOptions);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse> rerank(io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(getChannel().newCall(getRerankMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse> getSupportedRerankingProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(getChannel().newCall(getGetSupportedRerankingProvidersMethod(), getCallOptions()), request);
        }
    }

    private static final int METHODID_RERANK = 0;

    private static final int METHODID_GET_SUPPORTED_RERANKING_PROVIDERS = 1;

    private static final class MethodHandlers<Req, Resp> implements io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>, io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>, io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>, io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {

        private final AsyncService serviceImpl;

        private final int methodId;

        MethodHandlers(AsyncService serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch(methodId) {
                case METHODID_RERANK:
                    serviceImpl.rerank((io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse>) responseObserver);
                    break;
                case METHODID_GET_SUPPORTED_RERANKING_PROVIDERS:
                    serviceImpl.getSupportedRerankingProviders((io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse>) responseObserver);
                    break;
                default:
                    throw new AssertionError();
            }
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public io.grpc.stub.StreamObserver<Req> invoke(io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch(methodId) {
                default:
                    throw new AssertionError();
            }
        }
    }

    public static io.grpc.ServerServiceDefinition bindService(AsyncService service) {
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor()).addMethod(getRerankMethod(), io.grpc.stub.ServerCalls.asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.ProviderRerankingRequest, io.stargate.embedding.gateway.EmbeddingGateway.RerankingResponse>(service, METHODID_RERANK))).addMethod(getGetSupportedRerankingProvidersMethod(), io.grpc.stub.ServerCalls.asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedRerankingProvidersResponse>(service, METHODID_GET_SUPPORTED_RERANKING_PROVIDERS))).build();
    }

    private static abstract class RerankingServiceBaseDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {

        RerankingServiceBaseDescriptorSupplier() {
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return io.stargate.embedding.gateway.EmbeddingGateway.getDescriptor();
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
            return getFileDescriptor().findServiceByName("RerankingService");
        }
    }

    private static final class RerankingServiceFileDescriptorSupplier extends RerankingServiceBaseDescriptorSupplier {

        RerankingServiceFileDescriptorSupplier() {
        }
    }

    private static final class RerankingServiceMethodDescriptorSupplier extends RerankingServiceBaseDescriptorSupplier implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {

        private final java.lang.String methodName;

        RerankingServiceMethodDescriptorSupplier(java.lang.String methodName) {
            this.methodName = methodName;
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
            return getServiceDescriptor().findMethodByName(methodName);
        }
    }

    private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        io.grpc.ServiceDescriptor result = serviceDescriptor;
        if (result == null) {
            synchronized (RerankingServiceGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME).setSchemaDescriptor(new RerankingServiceFileDescriptorSupplier()).addMethod(getRerankMethod()).addMethod(getGetSupportedRerankingProvidersMethod()).build();
                }
            }
        }
        return result;
    }
}
