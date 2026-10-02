package io.stargate.sgv2.jsonapi.service.operation.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorFactory;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.ResponseData;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.DatabaseException;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDefSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class FindCollectionCandidateOperationTest extends OperationTestBase {
  @Inject ObjectMapper objectMapper;

  @Test
  void followsUnderfilledFirstPage() {
    var executor = mock(QueryExecutor.class);
    var states = stubReadPages(executor, page("second", rows(0, 1)), page(null, rows(1, 4)));

    var result = execute(candidateOperation(5, 4), executor);

    assertIds(result, "doc0", "doc1", "doc2", "doc3", "doc4");
    assertThat(states).containsExactly(Optional.empty(), Optional.of(token("second")));
    assertNoCursor(result);
  }

  @Test
  void followsEmptyIntermediatePage() {
    var executor = mock(QueryExecutor.class);
    var states =
        stubReadPages(
            executor, page("second", rows(0, 1)), page("third", List.of()), page(null, rows(1, 2)));

    var result = execute(candidateOperation(3, 2), executor);

    assertIds(result, "doc0", "doc1", "doc2");
    assertThat(states)
        .containsExactly(
            Optional.empty(), Optional.of(token("second")), Optional.of(token("third")));
    assertNoCursor(result);
  }

  @Test
  void returnsAvailableDocumentsOnEarlyExhaustion() {
    var executor = mock(QueryExecutor.class);
    stubReadPages(executor, page("second", rows(0, 1)), page(null, rows(1, 1)));

    var result = execute(candidateOperation(5, 2), executor);

    assertIds(result, "doc0", "doc1");
    verify(executor, times(2)).executeRead(eq(requestContext), any(), any(), eq(2));
    verifyNoMoreInteractions(executor);
    assertNoCursor(result);
  }

  @Test
  void stopsAtBudgetWithoutRequestingAnExtraPage() {
    var executor = mock(QueryExecutor.class);
    var states = stubReadPages(executor, page("second", rows(0, 2)), page("unused", rows(2, 2)));

    var result = execute(candidateOperation(3, 2), executor);

    assertIds(result, "doc0", "doc1", "doc2");
    assertThat(states).hasSize(2);
    assertNoCursor(result);
  }

  @Test
  void emptyFirstPageWithoutContinuationCompletes() {
    var executor = mock(QueryExecutor.class);
    stubReadPages(executor, page(null, List.of()));

    var result = execute(candidateOperation(3, 2), executor);

    assertThat(result.data().getResponseDocuments()).isEmpty();
    verify(executor).executeRead(eq(requestContext), any(), eq(Optional.empty()), eq(2));
    verifyNoMoreInteractions(executor);
    assertNoCursor(result);
  }

  @Test
  void laterPageDatabaseFailurePropagates() throws Exception {
    var executor = mock(QueryExecutor.class);
    var driverFailure =
        new ReadFailureException(
            mock(Node.class),
            ConsistencyLevel.ONE,
            1,
            0,
            1,
            false,
            Map.of(InetAddress.getByName("127.0.0.1"), 0));
    var failure =
        new CollectionDriverExceptionHandler(COLLECTION_SCHEMA_OBJECT, null).handle(driverFailure);
    var firstPage = page("second", rows(0, 1));
    when(executor.executeRead(eq(requestContext), any(), eq(Optional.empty()), anyInt()))
        .thenReturn(Uni.createFrom().item(firstPage));
    when(executor.executeRead(
            eq(requestContext), any(), eq(Optional.of(token("second"))), anyInt()))
        .thenReturn(Uni.createFrom().failure(failure));

    var actual = failure(candidateOperation(3, 2), executor);

    assertThat(actual).isSameAs(failure);
    assertThat(CommandErrorFactory.create(actual).errorCode())
        .isEqualTo(DatabaseException.Code.FAILED_READ_REQUEST.name());
  }

  @Test
  void laterPageInvalidDocumentFailsTheWholeRead() {
    var executor = mock(QueryExecutor.class);
    stubReadPages(
        executor, page("second", rows(0, 1)), page(null, List.of(row("invalid", "{invalid json"))));

    var actual = failure(candidateOperation(3, 2), executor);

    assertThat(CommandErrorFactory.create(actual).errorCode())
        .isEqualTo(DatabaseException.Code.DOCUMENT_FROM_DB_UNPARSEABLE.name());
  }

  @Test
  void subscriptionsStartWithFreshPagingStateAndBudget() {
    var executor = mock(QueryExecutor.class);
    var states = new ArrayList<Optional<String>>();
    when(executor.executeRead(eq(requestContext), any(), any(), anyInt()))
        .thenAnswer(
            invocation -> {
              Optional<String> state = invocation.getArgument(2);
              states.add(state);
              return Uni.createFrom()
                  .item(state.isEmpty() ? page("second", rows(0, 1)) : page(null, rows(1, 2)));
            });
    var execution = candidateOperation(3, 2).execute(requestContext, executor);

    assertIds(await(execution), "doc0", "doc1", "doc2");
    assertIds(await(execution), "doc0", "doc1", "doc2");
    assertThat(states)
        .containsExactly(
            Optional.empty(),
            Optional.of(token("second")),
            Optional.empty(),
            Optional.of(token("second")));
  }

  @Test
  void idInQueriesExecuteSequentiallyAndShareOneBudget() {
    var executor = mock(QueryExecutor.class);
    var calls = new AtomicInteger();
    var idsRead = new ArrayList<String>();
    var firstPage = new CompletableFuture<AsyncResultSet>();
    when(executor.executeRead(eq(requestContext), any(), any(), anyInt()))
        .thenAnswer(
            invocation -> {
              SimpleStatement statement = invocation.getArgument(1);
              var key =
                  (com.datastax.oss.driver.api.core.data.TupleValue)
                      statement.getPositionalValues().getFirst();
              String id = key.getString(1);
              idsRead.add(id);
              if (calls.incrementAndGet() == 1) {
                return Uni.createFrom().completionStage(firstPage);
              }
              return Uni.createFrom().item(page(null, List.of(row(id, document(id)))));
            });
    var expression = allDocuments();
    expression.addFilter(
        new IDCollectionFilter(
            IDCollectionFilter.Operator.IN,
            List.of(
                DocumentId.fromString("first"),
                DocumentId.fromString("second"),
                DocumentId.fromString("third"))));
    var operation =
        FindCollectionOperation.unsorted(
                COLLECTION_CONTEXT,
                expression,
                DocumentProjector.defaultProjector(),
                null,
                2,
                2,
                CollectionReadType.DOCUMENT,
                objectMapper,
                false)
            .withReadMode(CollectionReadMode.CANDIDATES);

    var subscriber =
        operation
            .execute(requestContext, executor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());
    assertThat(calls).hasValue(1);
    firstPage.complete(page(null, List.of(row(idsRead.getFirst(), document(idsRead.getFirst())))));
    var result = subscriber.awaitItem().getItem().get();

    assertThat(calls).hasValue(2);
    assertThat(result.data().getResponseDocuments())
        .extracting(doc -> doc.path("_id").asText())
        .containsExactlyElementsOf(idsRead);
    assertNoCursor(result);
  }

  @Test
  void sortedIdInQueriesExecuteSequentiallyAndScanAllQueriesBeforeLimitingResults() {
    var executor = mock(QueryExecutor.class);
    var calls = new AtomicInteger();
    var idsRead = new ArrayList<String>();
    var firstPage = new CompletableFuture<AsyncResultSet>();
    when(executor.executeRead(eq(requestContext), any(), any(), anyInt()))
        .thenAnswer(
            invocation -> {
              SimpleStatement statement = invocation.getArgument(1);
              var key =
                  (com.datastax.oss.driver.api.core.data.TupleValue)
                      statement.getPositionalValues().getFirst();
              String id = key.getString(1);
              idsRead.add(id);
              int call = calls.incrementAndGet();
              if (call == 1) {
                return Uni.createFrom().completionStage(firstPage);
              }
              var row = row(id, document(id));
              when(row.getBigDecimal(4)).thenReturn(BigDecimal.valueOf(call));
              return Uni.createFrom().item(page(null, List.of(row)));
            });
    var expression = allDocuments();
    expression.addFilter(
        new IDCollectionFilter(
            IDCollectionFilter.Operator.IN,
            List.of(
                DocumentId.fromString("first"),
                DocumentId.fromString("second"),
                DocumentId.fromString("third"))));
    var operation =
        FindCollectionOperation.sorted(
                COLLECTION_CONTEXT,
                expression,
                DocumentProjector.defaultProjector(),
                null,
                2,
                50,
                CollectionReadType.SORTED_DOCUMENT,
                objectMapper,
                List.of(new FindCollectionOperation.OrderBy("position", true)),
                0,
                4,
                false)
            .withReadMode(CollectionReadMode.CANDIDATES);

    var subscriber =
        operation
            .execute(requestContext, executor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());
    assertThat(calls).hasValue(1);
    var first = row(idsRead.getFirst(), document(idsRead.getFirst()));
    when(first.getBigDecimal(4)).thenReturn(BigDecimal.TEN);
    firstPage.complete(page(null, List.of(first)));
    var result = subscriber.awaitItem().getItem().get();

    assertThat(calls).hasValue(3);
    assertIds(result, idsRead.get(1), idsRead.get(2));
    assertNoCursor(result);
  }

  @Test
  void annPagesPreserveOrderAndSimilarity() {
    var executor = mock(QueryExecutor.class);
    var first = row("nearest", document("nearest"));
    var second = row("next", document("next"));
    when(first.getFloat(3)).thenReturn(0.95f);
    when(second.getFloat(3)).thenReturn(0.75f);
    var firstPage = page("second", List.of(first));
    var secondPage = page(null, List.of(second));
    when(executor.executeVectorSearch(eq(requestContext), any(), eq(Optional.empty()), anyInt()))
        .thenReturn(Uni.createFrom().item(firstPage));
    when(executor.executeVectorSearch(
            eq(requestContext), any(), eq(Optional.of(token("second"))), anyInt()))
        .thenReturn(Uni.createFrom().item(secondPage));
    var operation =
        FindCollectionOperation.vsearch(
                vectorContext(),
                allDocuments(),
                DocumentProjector.defaultProjectorWithSimilarity(),
                null,
                2,
                2,
                CollectionReadType.DOCUMENT,
                objectMapper,
                new float[] {1, 0},
                false)
            .withReadMode(CollectionReadMode.CANDIDATES);

    var result = execute(operation, executor);

    assertIds(result, "nearest", "next");
    assertThat(result.data().getResponseDocuments())
        .extracting(doc -> (float) doc.path("$similarity").asDouble())
        .containsExactly(0.95f, 0.75f);
    verify(executor, times(2)).executeVectorSearch(eq(requestContext), any(), any(), eq(2));
    verifyNoMoreInteractions(executor);
    assertNoCursor(result);
  }

  @Test
  void bm25PagesPreserveDatabaseRankOrder() {
    var executor = mock(QueryExecutor.class);
    var states =
        stubReadPages(
            executor,
            page("second", List.of(row("best", document("best")))),
            page(null, List.of(row("next", document("next")))));
    var operation =
        FindCollectionOperation.bm25Multi(
                COLLECTION_CONTEXT,
                allDocuments(),
                DocumentProjector.defaultProjector(),
                null,
                2,
                2,
                CollectionReadType.DOCUMENT,
                objectMapper,
                SortExpression.collectionLexicalSort("search text"))
            .withReadMode(CollectionReadMode.CANDIDATES);

    var result = execute(operation, executor);

    assertIds(result, "best", "next");
    assertThat(states).containsExactly(Optional.empty(), Optional.of(token("second")));
    assertNoCursor(result);
  }

  @Test
  void unsortedCandidatesIncludeDocumentsBeyondFirstFifty() {
    var executor = mock(QueryExecutor.class);
    stubReadPages(executor, page("second", rows(0, 50)), page(null, rows(50, 10)));

    var result = execute(candidateOperation(60, 50), executor);

    assertThat(result.data().getResponseDocuments())
        .extracting(doc -> doc.path("_id").asText())
        .containsExactlyElementsOf(IntStream.range(0, 60).mapToObj(i -> "doc" + i).toList());
    assertNoCursor(result);
  }

  @Test
  void sortedCandidatesScanAllPagesAndRetainRequestedCountAboveFifty() {
    var executor = mock(QueryExecutor.class);
    var states =
        stubReadPages(executor, page("second", sortedRows(10, 50)), page(null, sortedRows(0, 10)));

    var result = execute(sortedOperation(60, 101), executor);

    assertThat(result.data().getResponseDocuments())
        .extracting(doc -> doc.path("_id").asText())
        .containsExactlyElementsOf(IntStream.range(0, 60).mapToObj(i -> "doc" + i).toList());
    assertThat(states).containsExactly(Optional.empty(), Optional.of(token("second")));
    assertNoCursor(result);
  }

  @Test
  void sortedCandidatesStillScanPastResultBudgetToFindBestDocuments() {
    var executor = mock(QueryExecutor.class);
    stubReadPages(executor, page("second", sortedRows(10, 3)), page(null, sortedRows(0, 2)));

    var result = execute(sortedOperation(2, 101), executor);

    assertIds(result, "doc0", "doc1");
    verify(executor, times(2)).executeRead(eq(requestContext), any(), any(), eq(50));
    verifyNoMoreInteractions(executor);
    assertNoCursor(result);
  }

  @Test
  void sortedCandidatesRetainOverloadSafeguardAcrossPages() {
    var executor = mock(QueryExecutor.class);
    stubReadPages(executor, page("second", sortedRows(0, 3)), page(null, sortedRows(3, 3)));

    var actual = failure(sortedOperation(2, 6), executor);

    assertThat(CommandErrorFactory.create(actual).errorCode())
        .isEqualTo(SortException.Code.OVERLOADED_SORT_ROW_LIMIT.name());
  }

  @Test
  void sortedSubscriptionsStartWithFreshScanCounter() {
    var executor = mock(QueryExecutor.class);
    when(executor.executeRead(eq(requestContext), any(), eq(Optional.empty()), anyInt()))
        .thenAnswer(ignored -> Uni.createFrom().item(page(null, sortedRows(0, 3))));
    var execution = sortedOperation(2, 4).execute(requestContext, executor);

    assertIds(await(execution), "doc0", "doc1");
    assertIds(await(execution), "doc0", "doc1");
    verify(executor, times(2)).executeRead(eq(requestContext), any(), eq(Optional.empty()), eq(50));
  }

  @Test
  void ordinaryFindRetainsSinglePageAndCursor() {
    var executor = mock(QueryExecutor.class);
    stubReadPages(executor, page("second", rows(0, 2)));
    var operation = unsortedOperation(60, 50);

    var result = execute(operation, executor);

    assertThat(operation.readMode()).isEqualTo(CollectionReadMode.SINGLE_PAGE);
    assertIds(result, "doc0", "doc1");
    assertThat(((ResponseData.MultiResponseData) result.data()).nextPageState())
        .isEqualTo(token("second"));
    verify(executor).executeRead(eq(requestContext), any(), eq(Optional.empty()), eq(50));
    verifyNoMoreInteractions(executor);
  }

  private FindCollectionOperation candidateOperation(int limit, int pageSize) {
    return unsortedOperation(limit, pageSize).withReadMode(CollectionReadMode.CANDIDATES);
  }

  private FindCollectionOperation unsortedOperation(int limit, int pageSize) {
    return FindCollectionOperation.unsorted(
        COLLECTION_CONTEXT,
        allDocuments(),
        DocumentProjector.defaultProjector(),
        null,
        limit,
        pageSize,
        CollectionReadType.DOCUMENT,
        objectMapper,
        false);
  }

  private FindCollectionOperation sortedOperation(int limit, int errorLimit) {
    return FindCollectionOperation.sorted(
            COLLECTION_CONTEXT,
            allDocuments(),
            DocumentProjector.defaultProjector(),
            null,
            limit,
            50,
            CollectionReadType.SORTED_DOCUMENT,
            objectMapper,
            List.of(new FindCollectionOperation.OrderBy("position", true)),
            0,
            errorLimit,
            false)
        .withReadMode(CollectionReadMode.CANDIDATES);
  }

  private CommandContext<CollectionSchemaObject> vectorContext() {
    return TEST_CONSTANTS.collectionContext(
        "candidateVectorRead",
        new CollectionSchemaObject(
            TEST_CONSTANTS.COLLECTION_IDENTIFIER,
            IdConfig.defaultIdConfig(),
            VectorConfig.fromColumnDefinitions(
                List.of(
                    new VectorColumnDefinition(
                        DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                        -1,
                        SimilarityFunction.COSINE,
                        EmbeddingSourceModel.OTHER,
                        null))),
            null,
            CollectionLexicalDefSchemaFactory.FOR_TESTING_DISABLED.currentVersion(null),
            CollectionRerankDefSchemaFactory.FOR_TESTING_DISABLED.currentVersion(null)),
        jsonProcessingMetricsReporter,
        null);
  }

  private DBLogicalExpression allDocuments() {
    return new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
  }

  private List<Optional<String>> stubReadPages(QueryExecutor executor, AsyncResultSet... pages) {
    var states = new ArrayList<Optional<String>>();
    when(executor.executeRead(eq(requestContext), any(), any(), anyInt()))
        .thenAnswer(
            invocation -> {
              states.add(invocation.getArgument(2));
              assertThat(states.size())
                  .as("unexpected page read")
                  .isLessThanOrEqualTo(pages.length);
              return Uni.createFrom().item(pages[states.size() - 1]);
            });
    return states;
  }

  private AsyncResultSet page(String nextPage, List<Row> rows) {
    var result = mock(AsyncResultSet.class);
    when(result.remaining()).thenReturn(rows.size());
    when(result.currentPage()).thenReturn(rows);
    when(result.hasMorePages()).thenReturn(nextPage != null);
    if (nextPage != null) {
      var executionInfo = mock(ExecutionInfo.class);
      when(executionInfo.getPagingState())
          .thenReturn(ByteBuffer.wrap(nextPage.getBytes(StandardCharsets.UTF_8)));
      when(result.getExecutionInfo()).thenReturn(executionInfo);
    }
    return result;
  }

  private List<Row> rows(int start, int count) {
    return IntStream.range(start, start + count)
        .mapToObj(i -> row("doc" + i, document("doc" + i)))
        .toList();
  }

  private List<Row> sortedRows(int start, int count) {
    return IntStream.range(start, start + count)
        .mapToObj(
            i -> {
              var row = row("doc" + i, document("doc" + i));
              when(row.getBigDecimal(4)).thenReturn(BigDecimal.valueOf(i));
              return row;
            })
        .toList();
  }

  private Row row(String id, String json) {
    var row = mock(Row.class);
    when(row.getTupleValue(0))
        .thenReturn(DOC_KEY_TYPE.newValue((byte) DocumentConstants.KeyTypeId.TYPE_ID_STRING, id));
    when(row.getUuid(1)).thenReturn(UUID.randomUUID());
    when(row.getString(2)).thenReturn(json);
    return row;
  }

  private String document(String id) {
    return "{\"_id\":\"%s\",\"passage\":\"Candidate %s\"}".formatted(id, id);
  }

  private String token(String page) {
    return Base64.getEncoder().encodeToString(page.getBytes(StandardCharsets.UTF_8));
  }

  private CommandResult execute(FindCollectionOperation operation, QueryExecutor executor) {
    return await(operation.execute(requestContext, executor));
  }

  private CommandResult await(Uni<Supplier<CommandResult>> execution) {
    return execution
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem()
        .getItem()
        .get();
  }

  private Throwable failure(FindCollectionOperation operation, QueryExecutor executor) {
    return operation
        .execute(requestContext, executor)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitFailure()
        .getFailure();
  }

  private void assertIds(CommandResult result, String... ids) {
    assertThat(result.errors()).isEmpty();
    assertThat(result.data().getResponseDocuments())
        .extracting(doc -> doc.path("_id").asText())
        .containsExactly(ids);
  }

  private void assertNoCursor(CommandResult result) {
    assertThat(((ResponseData.MultiResponseData) result.data()).nextPageState()).isNull();
  }
}
