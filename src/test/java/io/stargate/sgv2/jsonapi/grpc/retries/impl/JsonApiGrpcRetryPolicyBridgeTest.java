package io.stargate.sgv2.jsonapi.grpc.retries.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.grpc.RetriableStargateBridge;
import io.stargate.sgv2.api.common.grpc.qualifier.Retriable;
import io.stargate.sgv2.common.bridge.BridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class JsonApiGrpcRetryPolicyBridgeTest extends BridgeTest {

  @Retriable @Inject RetriableStargateBridge bridge;

  @Test
  public void unavailable() {
    Status status = Status.UNAVAILABLE;
    StatusRuntimeException ex = new StatusRuntimeException(status);

    doAnswer(
            invocationOnMock -> {
              StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);

              observer.onError(ex);
              return null;
            })
        .when(bridgeService)
        .executeQuery(any(), any());

    QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
    Throwable failure =
        bridge
            .executeQuery(request)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(failure)
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE));

    // original call + retry
    verify(bridgeService, times(2)).executeQuery(eq(request), any());
  }

  @Test
  public void serverSideTimeout() {
    Metadata.Key<QueryOuterClass.WriteTimeout> key =
        ProtoUtils.keyForProto(QueryOuterClass.WriteTimeout.getDefaultInstance());
    QueryOuterClass.WriteTimeout value = QueryOuterClass.WriteTimeout.newBuilder().build();
    Metadata metadata = new Metadata();
    metadata.put(key, value);
    Status status = Status.DEADLINE_EXCEEDED;
    StatusRuntimeException ex = new StatusRuntimeException(status, metadata);

    doAnswer(
            invocationOnMock -> {
              StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
              observer.onError(ex);
              return null;
            })
        .when(bridgeService)
        .executeQuery(any(), any());

    QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
    Throwable failure =
        bridge
            .executeQuery(request)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(failure)
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED));

    // original call + retry
    verify(bridgeService, times(2)).executeQuery(eq(request), any());
  }

  @Test
  public void clientSideTimeout() {
    Status status = Status.DEADLINE_EXCEEDED;
    StatusRuntimeException ex = new StatusRuntimeException(status);

    doAnswer(
            invocationOnMock -> {
              StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
              observer.onError(ex);
              return null;
            })
        .when(bridgeService)
        .executeQuery(any(), any());

    QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
    Throwable failure =
        bridge
            .executeQuery(request)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitFailure()
            .getFailure();

    assertThat(failure)
        .isInstanceOfSatisfying(
            StatusRuntimeException.class,
            e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.DEADLINE_EXCEEDED));

    // original call only, no retry on client side
    verify(bridgeService).executeQuery(eq(request), any());
  }

  @Test
  public void noRetry() {
    QueryOuterClass.Response response = QueryOuterClass.Response.newBuilder().build();

    doAnswer(
            invocationOnMock -> {
              StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
              observer.onNext(response);
              observer.onCompleted();
              return null;
            })
        .when(bridgeService)
        .executeQuery(any(), any());

    QueryOuterClass.Query request = QueryOuterClass.Query.newBuilder().build();
    bridge
        .executeQuery(request)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem()
        .assertItem(response)
        .assertCompleted();

    // verify one call only
    verify(bridgeService).executeQuery(eq(request), any());
  }
}
