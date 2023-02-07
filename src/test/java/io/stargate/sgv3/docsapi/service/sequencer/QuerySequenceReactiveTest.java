package io.stargate.sgv3.docsapi.service.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.Mock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.common.bridge.BridgeTest;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.bridge.executor.ReactiveQueryExecutor;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import javax.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@QuarkusTest
@TestProfile(QuerySequenceReactiveTest.Profile.class)
class QuerySequenceReactiveTest extends BridgeTest {

  public static class Profile implements NoGlobalResourcesTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.queries.consistency.writes", "ONE")
          .put("stargate.queries.consistency.schema-changes", "ALL")
          .put("stargate.document.default-page-size", "30")
          .put("stargate.document.max-page-size", "50")
          .build();
    }
  }

  @GrpcClient("bridge")
  StargateBridge bridge;

  @Inject ReactiveQueryExecutor executor;

  @InjectMock StargateRequestInfo requestInfo;

  @BeforeEach
  public void init() {
    when(requestInfo.getStargateBridge()).thenReturn(bridge);
  }

  @Nested
  class QueryParams {

    @Mock Supplier<CommandResult> supplier;

    @Test
    public void readDefaults() {
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.query(query, QueryOptions.Type.READ).sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .singleElement()
          .satisfies(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().getConsistency().getValue())
                    .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
                assertThat(q.getParameters().getPageSize().getValue()).isEqualTo(30);
                assertThat(q.getParameters().hasPagingState()).isFalse();
              });
    }

    @Test
    public void writeDefaults() {
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("WRITE by Q1").build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.query(query, QueryOptions.Type.WRITE).sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .singleElement()
          .satisfies(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().getConsistency().getValue())
                    .isEqualTo(QueryOuterClass.Consistency.ONE);
                assertThat(q.getParameters().hasPageSize()).isFalse();
                assertThat(q.getParameters().hasPagingState()).isFalse();
              });
    }

    @Test
    public void consistencySchema() {
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SCHEMA by Q1").build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.query(query, QueryOptions.Type.SCHEMA).sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .singleElement()
          .satisfies(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().getConsistency().getValue())
                    .isEqualTo(QueryOuterClass.Consistency.ALL);
                assertThat(q.getParameters().hasPageSize()).isFalse();
                assertThat(q.getParameters().hasPagingState()).isFalse();
              });
    }

    @Test
    public void multiQueries() {
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
      QueryOuterClass.Query query2 =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.queries(List.of(query, query2), QueryOptions.Type.READ).sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService, times(2)).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .hasSize(2)
          .anySatisfy(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().getConsistency().getValue())
                    .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
                assertThat(q.getParameters().getPageSize().getValue()).isEqualTo(30);
                assertThat(q.getParameters().hasPagingState()).isFalse();
              })
          .anySatisfy(
              q -> {
                assertThat(q.getCql()).isEqualTo(query2.getCql());
                assertThat(q.getParameters().getConsistency().getValue())
                    .isEqualTo(QueryOuterClass.Consistency.LOCAL_QUORUM);
                assertThat(q.getParameters().getPageSize().getValue()).isEqualTo(30);
                assertThat(q.getParameters().hasPagingState()).isFalse();
              });
    }

    @Test
    public void consistencyIgnoredWhenSet() {
      QueryOuterClass.QueryParameters params =
          QueryOuterClass.QueryParameters.newBuilder()
              .setConsistency(
                  QueryOuterClass.ConsistencyValue.newBuilder()
                      .setValue(QueryOuterClass.Consistency.ANY)
                      .build())
              .build();
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").setParameters(params).build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.query(query, QueryOptions.Type.READ).sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .singleElement()
          .satisfies(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().getConsistency().getValue())
                    .isEqualTo(QueryOuterClass.Consistency.ANY);
              });
    }

    @Test
    public void pageSize() {
      int pageSize = 5;
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.query(query, QueryOptions.Type.READ)
              .withPageSize(pageSize)
              .sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .singleElement()
          .satisfies(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().getPageSize().getValue()).isEqualTo(pageSize);
                assertThat(q.getParameters().hasPagingState()).isFalse();
              });
    }

    @Test
    public void pageSizeOverMaxAllowed() {
      int pageSize = 1000;
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.query(query, QueryOptions.Type.READ)
              .withPageSize(pageSize)
              .sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .singleElement()
          .satisfies(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().getPageSize().getValue()).isEqualTo(50);
                assertThat(q.getParameters().hasPagingState()).isFalse();
              });
    }

    @Test
    public void pagingState() {
      String pagingState =
          Base64.getEncoder().encodeToString("state".getBytes(StandardCharsets.UTF_8));
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.query(query, QueryOptions.Type.READ)
              .withPagingState(pagingState)
              .sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .singleElement()
          .satisfies(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().getPagingState().getValue().toStringUtf8())
                    .isEqualTo("state");
              });
    }

    @Test
    public void pageSizeAndPagingStateIgnoredForNonReads() {
      String pagingState =
          Base64.getEncoder().encodeToString("state".getBytes(StandardCharsets.UTF_8));
      int pageSize = 10;
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
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

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.query(query, QueryOptions.Type.WRITE)
              .withPageSize(pageSize)
              .withPagingState(pagingState)
              .sink(r -> supplier);
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .singleElement()
          .satisfies(
              q -> {
                assertThat(q.getCql()).isEqualTo(query.getCql());
                assertThat(q.getParameters().hasPageSize()).isFalse();
                assertThat(q.getParameters().hasPagingState()).isFalse();
              });
    }
  }

  @Nested
  class Pipes {
    @Mock Supplier<CommandResult> supplier;

    @Test
    public void singlePipe() {
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
      QueryOuterClass.ResultSet resultSet = QueryOuterClass.ResultSet.newBuilder().build();
      QueryOuterClass.Response response =
          QueryOuterClass.Response.newBuilder().setResultSet(resultSet).build();
      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.empty()
              .then()
              .query(v -> query, QueryOptions.Type.READ)
              .sink(
                  r -> {
                    assertThat(r).isEqualTo(resultSet);
                    return supplier;
                  });
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues()).hasSize(1);
    }

    @Test
    public void multiPipe() {
      QueryOuterClass.Query query =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
      QueryOuterClass.Query query2 =
          QueryOuterClass.Query.newBuilder().setCql("SELECT by Q2").build();
      QueryOuterClass.ResultSet resultSet = QueryOuterClass.ResultSet.newBuilder().build();
      QueryOuterClass.Response response =
          QueryOuterClass.Response.newBuilder().setResultSet(resultSet).build();
      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      QuerySequenceSink<Supplier<CommandResult>> sequence =
          QuerySequence.empty()
              .then()
              .queries(v -> List.of(query, query2), QueryOptions.Type.READ)
              .sink(
                  r -> {
                    assertThat(r).hasSize(2).containsOnly(resultSet);
                    return supplier;
                  });
      sequence
          .reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService, times(2)).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .hasSize(2)
          .extracting(QueryOuterClass.Query::getCql)
          .containsOnly("SELECT by Q1", "SELECT by Q2");
    }
  }

  @Nested
  class CompositeOperations {

    @Mock Supplier<CommandResult> supplier1;

    @Mock Supplier<CommandResult> supplier2;

    @Mock Supplier<CommandResult> supplier3;

    QueryOuterClass.Query q1 = QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
    QueryOuterClass.Query q2 = QueryOuterClass.Query.newBuilder().setCql("SELECT by Q2").build();

    class Op1 implements Operation {

      @Override
      public QuerySequenceSink<Supplier<CommandResult>> getOperationSequence() {
        return extracted().sink(any -> supplier1);
      }

      public SingleQuerySequence<QueryOuterClass.ResultSet> extracted() {
        return QuerySequence.query(q1, QueryOptions.Type.READ);
      }
    }

    class Op2 implements Operation {

      @Override
      public QuerySequenceSink<Supplier<CommandResult>> getOperationSequence() {
        return extracted().sink(any -> null);
      }

      public SingleQuerySequence<QueryOuterClass.ResultSet> extracted() {
        return QuerySequence.query(q2, QueryOptions.Type.WRITE);
      }
    }

    @Test
    public void success() {
      // given two operation
      Op1 op1 = new Op1();
      Op2 op2 = new Op2();

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

      // when piping
      QuerySequenceSink<Supplier<CommandResult>> sink =
          op1.extracted().pipeTo(resultSet -> op2.extracted()).sink(any -> supplier2);

      sink.reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier2);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService, times(2)).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .extracting(QueryOuterClass.Query::getCql)
          .containsExactlyInAnyOrder("SELECT by Q1", "SELECT by Q2");
    }

    @Test
    public void failureInFirstOp() {
      // given two operation
      Op1 op1 = new Op1();
      Op2 op2 = new Op2();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                Status status = Status.NOT_FOUND;
                observer.onError(new StatusRuntimeException(status));
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      // when piping
      QuerySequenceSink<Supplier<CommandResult>> sink =
          op1.extracted().pipeTo(resultSet -> op2.extracted()).sink(any -> supplier2);

      sink.reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitFailure();

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .extracting(QueryOuterClass.Query::getCql)
          .containsExactlyInAnyOrder("SELECT by Q1");
    }

    @Test
    public void failureInFirstOpHandled() {
      // given two operation
      Op1 op1 = new Op1();
      Op2 op2 = new Op2();

      doAnswer(
              invocationOnMock -> {
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);
                Status status = Status.NOT_FOUND;
                observer.onError(new StatusRuntimeException(status));
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      // when piping
      QuerySequenceSink<Supplier<CommandResult>> sink =
          // go for op1
          op1.extracted()
              // add custom handler for composite op
              .withHandler((result, throwable) -> null == throwable)
              // pipe conditionally
              .pipeToSink(
                  success -> {
                    if (success) {
                      return op2.getOperationSequence();
                    } else {
                      return QuerySequence.empty().sink(any -> supplier3);
                    }
                  });

      sink.reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier3);

      // assert and verify
      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .extracting(QueryOuterClass.Query::getCql)
          .containsExactlyInAnyOrder("SELECT by Q1");
    }
  }

  @Nested
  class PartialFailures {

    @Mock Supplier<CommandResult> supplier;

    @Test
    public void handled() {
      // given
      QueryOptions opts = new QueryOptions(QueryOptions.Type.WRITE);
      QueryOuterClass.Query q1 = QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
      QueryOuterClass.Query q2 = QueryOuterClass.Query.newBuilder().setCql("SELECT by Q2").build();
      QueryOuterClass.Response r2 = QueryOuterClass.Response.newBuilder().build();

      // q1 fails, q2 responds fine
      doAnswer(
              invocationOnMock -> {
                QueryOuterClass.Query q = invocationOnMock.getArgument(0);
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);

                if (Objects.equals(q.getCql(), "SELECT by Q1")) {
                  Status status = Status.NOT_FOUND;
                  observer.onError(new StatusRuntimeException(status));
                } else {
                  observer.onNext(r2);
                  observer.onCompleted();
                }
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      // when
      QuerySequenceSink<Supplier<CommandResult>> sink =
          QuerySequence.queries(List.of(q1, q2), opts, (result, throwable, index) -> index)
              .then()
              .sink(
                  indexes -> {
                    assertThat(indexes).containsExactly(0, 1);
                    return supplier;
                  });

      sink.reactive()
          .execute(executor)
          .subscribe()
          .withSubscriber(UniAssertSubscriber.create())
          .awaitItem()
          .assertItem(supplier);

      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService, times(2)).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .extracting(QueryOuterClass.Query::getCql)
          .containsExactlyInAnyOrder("SELECT by Q1", "SELECT by Q2");
    }

    @Test
    public void notHandled() {
      // given
      QueryOptions opts = new QueryOptions(QueryOptions.Type.WRITE);
      QueryOuterClass.Query q1 = QueryOuterClass.Query.newBuilder().setCql("SELECT by Q1").build();
      QueryOuterClass.Query q2 = QueryOuterClass.Query.newBuilder().setCql("SELECT by Q2").build();
      QueryOuterClass.Response r2 = QueryOuterClass.Response.newBuilder().build();

      // q1 fails, q2 responds fine
      doAnswer(
              invocationOnMock -> {
                QueryOuterClass.Query q = invocationOnMock.getArgument(0);
                StreamObserver<QueryOuterClass.Response> observer = invocationOnMock.getArgument(1);

                if (Objects.equals(q.getCql(), "SELECT by Q1")) {
                  Status status = Status.NOT_FOUND;
                  observer.onError(new StatusRuntimeException(status));
                } else {
                  observer.onNext(r2);
                  observer.onCompleted();
                }
                return null;
              })
          .when(bridgeService)
          .executeQuery(any(), any());

      // when
      QuerySequenceSink<Supplier<CommandResult>> sink =
          QuerySequence.queries(List.of(q1, q2), opts).then().sink(result -> supplier);

      Throwable failure =
          sink.reactive()
              .execute(executor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(failure)
          .isInstanceOf(StatusRuntimeException.class)
          .hasFieldOrPropertyWithValue("status", Status.NOT_FOUND);

      ArgumentCaptor<QueryOuterClass.Query> captor =
          ArgumentCaptor.forClass(QueryOuterClass.Query.class);
      verify(bridgeService, times(2)).executeQuery(captor.capture(), any());
      verifyNoMoreInteractions(bridgeService);

      assertThat(captor.getAllValues())
          .extracting(QueryOuterClass.Query::getCql)
          .containsExactlyInAnyOrder("SELECT by Q1", "SELECT by Q2");
    }
  }
}
