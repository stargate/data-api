package io.stargate.embedding.gateway;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * The embedding gateway gPRC API to generate embeddings
 * </pre>
 */
@io.quarkus.Generated(value = "by gRPC proto compiler (version 1.65.1)", comments = "Source: embedding_gateway.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class EmbeddingServiceGrpc {

    private EmbeddingServiceGrpc() {
    }

    public static final java.lang.String SERVICE_NAME = "stargate.EmbeddingService";

    // Static method descriptors that strictly reflect the proto.
    private static volatile io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest, io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> getEmbedMethod;

    @io.grpc.stub.annotations.RpcMethod(fullMethodName = SERVICE_NAME + '/' + "Embed", requestType = io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest.class, responseType = io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse.class, methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest, io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> getEmbedMethod() {
        io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest, io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> getEmbedMethod;
        if ((getEmbedMethod = EmbeddingServiceGrpc.getEmbedMethod) == null) {
            synchronized (EmbeddingServiceGrpc.class) {
                if ((getEmbedMethod = EmbeddingServiceGrpc.getEmbedMethod) == null) {
                    EmbeddingServiceGrpc.getEmbedMethod = getEmbedMethod = io.grpc.MethodDescriptor.<io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest, io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse>newBuilder().setType(io.grpc.MethodDescriptor.MethodType.UNARY).setFullMethodName(generateFullMethodName(SERVICE_NAME, "Embed")).setSampledToLocalTracing(true).setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest.getDefaultInstance())).setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse.getDefaultInstance())).setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("Embed")).build();
                }
            }
        }
        return getEmbedMethod;
    }

    private static volatile io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getGetSupportedProvidersMethod;

    @io.grpc.stub.annotations.RpcMethod(fullMethodName = SERVICE_NAME + '/' + "GetSupportedProviders", requestType = io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest.class, responseType = io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse.class, methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getGetSupportedProvidersMethod() {
        io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getGetSupportedProvidersMethod;
        if ((getGetSupportedProvidersMethod = EmbeddingServiceGrpc.getGetSupportedProvidersMethod) == null) {
            synchronized (EmbeddingServiceGrpc.class) {
                if ((getGetSupportedProvidersMethod = EmbeddingServiceGrpc.getGetSupportedProvidersMethod) == null) {
                    EmbeddingServiceGrpc.getGetSupportedProvidersMethod = getGetSupportedProvidersMethod = io.grpc.MethodDescriptor.<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse>newBuilder().setType(io.grpc.MethodDescriptor.MethodType.UNARY).setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSupportedProviders")).setSampledToLocalTracing(true).setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest.getDefaultInstance())).setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse.getDefaultInstance())).setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("GetSupportedProviders")).build();
                }
            }
        }
        return getGetSupportedProvidersMethod;
    }

    private static volatile io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest, io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> getValidateCredentialMethod;

    @io.grpc.stub.annotations.RpcMethod(fullMethodName = SERVICE_NAME + '/' + "ValidateCredential", requestType = io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest.class, responseType = io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse.class, methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest, io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> getValidateCredentialMethod() {
        io.grpc.MethodDescriptor<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest, io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> getValidateCredentialMethod;
        if ((getValidateCredentialMethod = EmbeddingServiceGrpc.getValidateCredentialMethod) == null) {
            synchronized (EmbeddingServiceGrpc.class) {
                if ((getValidateCredentialMethod = EmbeddingServiceGrpc.getValidateCredentialMethod) == null) {
                    EmbeddingServiceGrpc.getValidateCredentialMethod = getValidateCredentialMethod = io.grpc.MethodDescriptor.<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest, io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse>newBuilder().setType(io.grpc.MethodDescriptor.MethodType.UNARY).setFullMethodName(generateFullMethodName(SERVICE_NAME, "ValidateCredential")).setSampledToLocalTracing(true).setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest.getDefaultInstance())).setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse.getDefaultInstance())).setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("ValidateCredential")).build();
                }
            }
        }
        return getValidateCredentialMethod;
    }

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static EmbeddingServiceStub newStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceStub> factory = new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceStub>() {

            @java.lang.Override
            public EmbeddingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                return new EmbeddingServiceStub(channel, callOptions);
            }
        };
        return EmbeddingServiceStub.newStub(factory, channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static EmbeddingServiceBlockingStub newBlockingStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingStub> factory = new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingStub>() {

            @java.lang.Override
            public EmbeddingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                return new EmbeddingServiceBlockingStub(channel, callOptions);
            }
        };
        return EmbeddingServiceBlockingStub.newStub(factory, channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary calls on the service
     */
    public static EmbeddingServiceFutureStub newFutureStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceFutureStub> factory = new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceFutureStub>() {

            @java.lang.Override
            public EmbeddingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                return new EmbeddingServiceFutureStub(channel, callOptions);
            }
        };
        return EmbeddingServiceFutureStub.newStub(factory, channel);
    }

    /**
     * <pre>
     * The embedding gateway gPRC API to generate embeddings
     * </pre>
     */
    public interface AsyncService {

        /**
         */
        default void embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEmbedMethod(), responseObserver);
        }

        /**
         */
        default void getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSupportedProvidersMethod(), responseObserver);
        }

        /**
         */
        default void validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> responseObserver) {
            io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getValidateCredentialMethod(), responseObserver);
        }
    }

    /**
     * Base class for the server implementation of the service EmbeddingService.
     * <pre>
     * The embedding gateway gPRC API to generate embeddings
     * </pre>
     */
    public static abstract class EmbeddingServiceImplBase implements io.grpc.BindableService, AsyncService {

        @java.lang.Override
        public io.grpc.ServerServiceDefinition bindService() {
            return EmbeddingServiceGrpc.bindService(this);
        }
    }

    /**
     * A stub to allow clients to do asynchronous rpc calls to service EmbeddingService.
     * <pre>
     * The embedding gateway gPRC API to generate embeddings
     * </pre>
     */
    public static class EmbeddingServiceStub extends io.grpc.stub.AbstractAsyncStub<EmbeddingServiceStub> {

        private EmbeddingServiceStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected EmbeddingServiceStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new EmbeddingServiceStub(channel, callOptions);
        }

        /**
         */
        public void embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(getChannel().newCall(getEmbedMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(getChannel().newCall(getGetSupportedProvidersMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         */
        public void validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request, io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> responseObserver) {
            io.grpc.stub.ClientCalls.asyncUnaryCall(getChannel().newCall(getValidateCredentialMethod(), getCallOptions()), request, responseObserver);
        }
    }

    /**
     * A stub to allow clients to do synchronous rpc calls to service EmbeddingService.
     * <pre>
     * The embedding gateway gPRC API to generate embeddings
     * </pre>
     */
    public static class EmbeddingServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<EmbeddingServiceBlockingStub> {

        private EmbeddingServiceBlockingStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected EmbeddingServiceBlockingStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new EmbeddingServiceBlockingStub(channel, callOptions);
        }

        /**
         */
        public io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(getChannel(), getEmbedMethod(), getCallOptions(), request);
        }

        /**
         */
        public io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(getChannel(), getGetSupportedProvidersMethod(), getCallOptions(), request);
        }

        /**
         */
        public io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request) {
            return io.grpc.stub.ClientCalls.blockingUnaryCall(getChannel(), getValidateCredentialMethod(), getCallOptions(), request);
        }
    }

    /**
     * A stub to allow clients to do ListenableFuture-style rpc calls to service EmbeddingService.
     * <pre>
     * The embedding gateway gPRC API to generate embeddings
     * </pre>
     */
    public static class EmbeddingServiceFutureStub extends io.grpc.stub.AbstractFutureStub<EmbeddingServiceFutureStub> {

        private EmbeddingServiceFutureStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected EmbeddingServiceFutureStub build(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new EmbeddingServiceFutureStub(channel, callOptions);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse> embed(io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(getChannel().newCall(getEmbedMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse> getSupportedProviders(io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(getChannel().newCall(getGetSupportedProvidersMethod(), getCallOptions()), request);
        }

        /**
         */
        public com.google.common.util.concurrent.ListenableFuture<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse> validateCredential(io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest request) {
            return io.grpc.stub.ClientCalls.futureUnaryCall(getChannel().newCall(getValidateCredentialMethod(), getCallOptions()), request);
        }
    }

    private static final int METHODID_EMBED = 0;

    private static final int METHODID_GET_SUPPORTED_PROVIDERS = 1;

    private static final int METHODID_VALIDATE_CREDENTIAL = 2;

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
                case METHODID_EMBED:
                    serviceImpl.embed((io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse>) responseObserver);
                    break;
                case METHODID_GET_SUPPORTED_PROVIDERS:
                    serviceImpl.getSupportedProviders((io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse>) responseObserver);
                    break;
                case METHODID_VALIDATE_CREDENTIAL:
                    serviceImpl.validateCredential((io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest) request, (io.grpc.stub.StreamObserver<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse>) responseObserver);
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
        return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor()).addMethod(getEmbedMethod(), io.grpc.stub.ServerCalls.asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.ProviderEmbedRequest, io.stargate.embedding.gateway.EmbeddingGateway.EmbeddingResponse>(service, METHODID_EMBED))).addMethod(getGetSupportedProvidersMethod(), io.grpc.stub.ServerCalls.asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersRequest, io.stargate.embedding.gateway.EmbeddingGateway.GetSupportedProvidersResponse>(service, METHODID_GET_SUPPORTED_PROVIDERS))).addMethod(getValidateCredentialMethod(), io.grpc.stub.ServerCalls.asyncUnaryCall(new MethodHandlers<io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialRequest, io.stargate.embedding.gateway.EmbeddingGateway.ValidateCredentialResponse>(service, METHODID_VALIDATE_CREDENTIAL))).build();
    }

    private static abstract class EmbeddingServiceBaseDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {

        EmbeddingServiceBaseDescriptorSupplier() {
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return io.stargate.embedding.gateway.EmbeddingGateway.getDescriptor();
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
            return getFileDescriptor().findServiceByName("EmbeddingService");
        }
    }

    private static final class EmbeddingServiceFileDescriptorSupplier extends EmbeddingServiceBaseDescriptorSupplier {

        EmbeddingServiceFileDescriptorSupplier() {
        }
    }

    private static final class EmbeddingServiceMethodDescriptorSupplier extends EmbeddingServiceBaseDescriptorSupplier implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {

        private final java.lang.String methodName;

        EmbeddingServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
            synchronized (EmbeddingServiceGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME).setSchemaDescriptor(new EmbeddingServiceFileDescriptorSupplier()).addMethod(getEmbedMethod()).addMethod(getGetSupportedProvidersMethod()).addMethod(getValidateCredentialMethod()).build();
                }
            }
        }
        return result;
    }
}
