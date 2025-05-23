package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.*;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindCollectionOperationTest extends OperationTestBase {
  private CommandContext<CollectionSchemaObject> COMMAND_CONTEXT;

  private CommandContext<CollectionSchemaObject> VECTOR_COMMAND_CONTEXT;
  private TestConstants testConstants = new TestConstants();

  private final ColumnDefinitions KEY_TXID_JSON_COLUMNS =
      buildColumnDefs(
          TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"), TestColumn.ofVarchar("doc_json"));

  @Inject ObjectMapper objectMapper;

  @BeforeEach
  public void beforeEach() {
    super.beforeEach();
    // TODO: a lot of these test create the same command context, these should be in the base class
    // leaving as new objects for now, they can prob be reused

    COMMAND_CONTEXT = createCommandContextWithCommandName("testCommand");
    VECTOR_COMMAND_CONTEXT =
        testConstants.collectionContext(
            "testCommand",
            new CollectionSchemaObject(
                SCHEMA_OBJECT_NAME,
                null,
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
                CollectionLexicalConfig.configForDisabled(),
                CollectionRerankDef.configForPreRerankingCollection()),
            jsonProcessingMetricsReporter,
            null);
  }

  @Nested
  class Execute {

    @Test
    public void findAll() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1"
                  }
                  """;
      String doc2 =
          """
                  {
                    "_id": "doc2",
                    "username": "user2"
                  }
                  """;
      CommandContext commandContext = createCommandContextWithCommandName("jsonBytesReadCommand");
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      List<Row> rows =
          Arrays.asList(
              resultRow(0, "doc1", UUID.randomUUID(), doc1),
              resultRow(1, "doc2", UUID.randomUUID(), doc2));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      FindCollectionOperation operation =
          FindCollectionOperation.unsorted(
              commandContext,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              20,
              20,
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(2)
          .containsOnly(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();

      // verify metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> jsonBytesReadMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_bytes_read")
                          && !line.startsWith("json_bytes_read_bucket")
                          && !line.contains("quantile")
                          && line.contains("jsonBytesReadCommand"))
              .toList();
      // should have three metrics in total
      assertThat(jsonBytesReadMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"jsonBytesReadCommand\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");
                    });
              });
      // verify count metric -- command called once, should be one
      List<String> jsonDocsReadCountMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_read_count")
                          && line.contains("jsonBytesReadCommand"))
              .toList();
      assertThat(jsonDocsReadCountMetrics).hasSize(1);
      jsonDocsReadCountMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(1.0);
          });
      // verify sum metric -- read two docs, should be two
      List<String> jsonDocsReadSumMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_read_sum")
                          && line.contains("jsonBytesReadCommand"))
              .toList();
      assertThat(jsonDocsReadSumMetrics).hasSize(1);
      jsonDocsReadSumMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(2.0);
          });
    }

    @Test
    public void byIdWithInOperator() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 2"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
              {
                "_id": "doc1",
                "username": "user1"
              }
              """;
      String doc2 =
          """
              {
                "_id": "doc2",
                "username": "user2"
              }
              """;

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      SimpleStatement stmt2 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc2"));
      List<Row> rows2 = Arrays.asList(resultRow(0, "doc2", UUID.randomUUID(), doc2));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      final AtomicInteger callCount2 = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new IDCollectionFilter(
              IDCollectionFilter.Operator.IN,
              List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"))));

      FindCollectionOperation operation =
          FindCollectionOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              2,
              2,
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(2)
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.errors()).isNullOrEmpty();
      assertThat(result.status()).isEmpty();
      ;
    }

    @Test
    public void findnonVsearchWithSortVectorFlag() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 2"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1"
                      }
                      """;
      String doc2 =
          """
                      {
                        "_id": "doc2",
                        "username": "user2"
                      }
                      """;

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      SimpleStatement stmt2 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc2"));
      List<Row> rows2 = Arrays.asList(resultRow(0, "doc2", UUID.randomUUID(), doc2));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      final AtomicInteger callCount2 = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new IDCollectionFilter(
              IDCollectionFilter.Operator.IN,
              List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"))));

      FindCollectionOperation operation =
          FindCollectionOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              2,
              2,
              CollectionReadType.DOCUMENT,
              objectMapper,
              true);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(2)
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNotNull();
      assertThat(result.errors()).isNullOrEmpty();
      assertThat(result.status()).isNotNull();
      assertThat(result.status().containsKey(CommandStatus.SORT_VECTOR)).isTrue();
      assertThat(result.status().get(CommandStatus.SORT_VECTOR)).isNull();
    }

    @Test
    public void byIdWithInEmptyArray() {
      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(new IDCollectionFilter(IDCollectionFilter.Operator.IN, List.of()));
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      FindCollectionOperation operation =
          FindCollectionOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              2,
              2,
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments()).hasSize(0);
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void byIdWithInAndOtherOperator() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 2"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1"
                  }
                  """;
      String doc2 =
          """
                  {
                    "_id": "doc2",
                    "username": "user2"
                  }
                  """;

      final String textFilterValue = "username " + new DocValueHasher().getHash("user1").hash();
      SimpleStatement stmt1 =
          SimpleStatement.newInstance(
              collectionReadCql, boundKeyForStatement("doc1"), textFilterValue);
      List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      SimpleStatement stmt2 =
          SimpleStatement.newInstance(
              collectionReadCql, boundKeyForStatement("doc2"), textFilterValue);
      List<Row> rows2 = Arrays.asList(resultRow(0, "doc2", UUID.randomUUID(), doc2));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      final AtomicInteger callCount2 = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new IDCollectionFilter(
              IDCollectionFilter.Operator.IN,
              List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"))));
      implicitAnd.addFilter(
          new TextCollectionFilter("username", TextCollectionFilter.Operator.EQ, "user1"));

      FindCollectionOperation operation =
          FindCollectionOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              2,
              2,
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);
      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(2)
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findOneByIdWithInOperator() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1"
                  }
                  """;
      String doc2 =
          """
                  {
                    "_id": "doc2",
                    "username": "user2"
                  }
                  """;
      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      SimpleStatement stmt2 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc2"));
      List<Row> rows2 = Arrays.asList(resultRow(0, "doc2", UUID.randomUUID(), doc2));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      final AtomicInteger callCount2 = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new IDCollectionFilter(
              IDCollectionFilter.Operator.IN,
              List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"))));

      FindCollectionOperation operation =
          FindCollectionOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              1,
              2,
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);
      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsAnyOf(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithId() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
            {
              "_id": "doc1",
              "username": "user1"
            }
            """;

      SimpleStatement stmt =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new IDCollectionFilter(IDCollectionFilter.Operator.EQ, DocumentId.fromString("doc1")));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithIdNoData() {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement stmt =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, Arrays.asList(), null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new IDCollectionFilter(IDCollectionFilter.Operator.EQ, DocumentId.fromString("doc1")));

      FindCollectionOperation operation =
          FindCollectionOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              1,
              1,
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments()).isEmpty();
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithDynamic() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1"
                  }
                  """;

      final String textFilterValue = "username " + new DocValueHasher().getHash("user1").hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, textFilterValue);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new TextCollectionFilter("username", MapCollectionFilter.Operator.EQ, "user1"));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithDynamicGT() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_dbl_values[?] > ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "amount" : "200"
                      }
                      """;

      SimpleStatement stmt =
          SimpleStatement.newInstance(collectionReadCql, "amount", new BigDecimal(100));
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new NumberCollectionFilter(
              "amount", MapCollectionFilter.Operator.GT, new BigDecimal(100)));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithDynamicGTE() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_dbl_values[?] >= ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "amount" : "200"
                      }
                      """;

      SimpleStatement stmt =
          SimpleStatement.newInstance(collectionReadCql, "amount", new BigDecimal(200));
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new NumberCollectionFilter(
              "amount", MapCollectionFilter.Operator.GTE, new BigDecimal(200)));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithDynamicLT() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_timestamp_values[?] < ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "dob" : {"$date" : 1672531200000}
                      }
                      """;

      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "dob", Instant.ofEpochMilli(1672531200000L));
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new DateCollectionFilter(
              "dob", MapCollectionFilter.Operator.LT, new Date(1672531200000L)));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithDynamicLTE() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_timestamp_values[?] <= ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "dob" : {"$date" : 1672531200000}
                      }
                      """;

      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "dob", Instant.ofEpochMilli(1672531200000L));
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new DateCollectionFilter(
              "dob", MapCollectionFilter.Operator.LTE, new Date(1672531200000L)));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithBooleanFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "registration_active" : true
                  }
                  """;

      final String booleanFilterValue =
          "registration_active " + new DocValueHasher().getHash(true).hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, booleanFilterValue);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new BoolCollectionFilter("registration_active", MapCollectionFilter.Operator.EQ, true));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithDateFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "registration_active" : true,
                        "date_field" : {"$date" : 1672531200000}
                      }
                      """;
      final String dateFilterValue =
          "date_field " + new DocValueHasher().getHash(Instant.ofEpochMilli(1672531200000L)).hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, dateFilterValue);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new DateCollectionFilter(
              "date_field", MapCollectionFilter.Operator.EQ, new Date(1672531200000L)));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithExistsFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE exist_keys CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "registration_active" : true
                  }
                  """;

      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, "registration_active");
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(new ExistsCollectionFilter("registration_active", true));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithAllFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (array_contains CONTAINS ? AND array_contains CONTAINS ?) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "registration_active" : true,
                    "tags": ["tag1", "tag2"]
                  }
                  """;

      SimpleStatement stmt =
          SimpleStatement.newInstance(collectionReadCql, "tags Stag1", "tags Stag2");
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(new AllCollectionFilter("tags", List.of("tag1", "tag2"), false));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findOrWithAllFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (array_contains CONTAINS ? OR (array_contains CONTAINS ? AND array_contains CONTAINS ?)) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "registration_active" : true,
                        "tags": ["tag1", "tag2"]
                      }
                      """;

      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username Suser1", "tags Stag1", "tags Stag2");
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      DBLogicalExpression explicitOr =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.OR);
      explicitOr.addFilter(
          new TextCollectionFilter("username", MapCollectionFilter.Operator.EQ, "user1"));
      explicitOr.addFilter(new AllCollectionFilter("tags", List.of("tag1", "tag2"), false));
      implicitAnd.addSubExpressionReturnCurrent(explicitOr);

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findOrWithAllFilterWithNegation() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (array_contains CONTAINS ? OR (array_contains NOT CONTAINS ? OR array_contains NOT CONTAINS ?)) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                          {
                            "_id": "doc1",
                            "username": "user1",
                            "registration_active" : true,
                            "tags": ["tag1", "tag2"]
                          }
                          """;

      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username Suser1", "tags Stag1", "tags Stag2");
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      DBLogicalExpression explicitOr =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.OR);
      explicitOr.addFilter(
          new TextCollectionFilter("username", MapCollectionFilter.Operator.EQ, "user1"));
      explicitOr.addFilter(new AllCollectionFilter("tags", List.of("tag1", "tag2"), true));
      implicitAnd.addSubExpressionReturnCurrent(explicitOr);

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithSizeFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_size[?] = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "registration_active" : true,
                    "tags" : ["tag1","tag2"]
                  }
                  """;

      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, "tags", 2);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new SizeCollectionFilter("tags", MapCollectionFilter.Operator.MAP_EQUALS, 2));

      FindCollectionOperation operation =
          FindCollectionOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              1,
              1,
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithArrayEqualFilter() throws Exception {
      // Due to trimming of indexes, former "array_equals" moved under "query_text_values":
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_text_values[?] = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "registration_active" : true,
                    "tags" : ["tag1","tag2"]
                  }
                  """;

      final String tagsHash = new DocValueHasher().getHash(List.of("tag1", "tag2")).hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, "tags", tagsHash);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new ArrayEqualsCollectionFilter(
              new DocValueHasher(),
              "tags",
              List.of("tag1", "tag2"),
              MapCollectionFilter.Operator.MAP_EQUALS));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithArrayNotEqualFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_text_values[?] != ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "registration_active" : true,
                        "tags" : ["tag1","tag3"] }
                      }
                      """;

      final String tagsHash = new DocValueHasher().getHash(List.of("tag1", "tag3")).hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, "tags", tagsHash);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new ArrayEqualsCollectionFilter(
              new DocValueHasher(),
              "tags",
              List.of("tag1", "tag3"),
              MapCollectionFilter.Operator.MAP_NOT_EQUALS));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithSubDocEqualFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_text_values[?] = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "registration_active" : true,
                    "sub_doc" : {"col":"val"}
                  }
                  """;
      final String hash = new DocValueHasher().getHash(Map.of("col", "val")).hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, "sub_doc", hash);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new SubDocEqualsCollectionFilter(
              new DocValueHasher(),
              "sub_doc",
              Map.of("col", "val"),
              MapCollectionFilter.Operator.MAP_EQUALS));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findWithSubDocNotEqualFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE query_text_values[?] != ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
                        "registration_active" : true,
                        "sub_doc" : {"col":"invalid"}
                      }
                      """;
      final String hash = new DocValueHasher().getHash(Map.of("col", "val")).hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, "sub_doc", hash);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new SubDocEqualsCollectionFilter(
              new DocValueHasher(),
              "sub_doc",
              Map.of("col", "val"),
              MapCollectionFilter.Operator.MAP_NOT_EQUALS));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    /////////////////////
    ///    FAILURES   ///
    /////////////////////

    @Test
    public void failurePropagated() {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      RuntimeException exception = new RuntimeException("Ivan breaks tests.");

      SimpleStatement stmt =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().failure(exception);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new IDCollectionFilter(IDCollectionFilter.Operator.EQ, DocumentId.fromString("doc1")));

      FindCollectionOperation operation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);

      Throwable failure =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      assertThat(failure).isEqualTo(exception);
    }

    @Test
    public void findAllSort() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

      String doc1 =
          """
                {
                  "_id": "doc1",
                  "username": "user1"
                }
                """;
      String doc2 =
          """
                {
                  "_id": "doc2",
                  "username": "user2"
                }
                """;
      String doc3 =
          """
                {
                  "_id": "doc3",
                  "username": "user3"
                }
                """;
      String doc4 =
          """
                {
                  "_id": "doc4",
                  "username": "user4"
                }
                """;
      String doc5 =
          """
                {
                  "_id": "doc5",
                  "username": "user5"
                }
                """;
      String doc6 =
          """
                {
                  "_id": "doc6",
                  "username": "user6"
                }
                """;

      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      ColumnDefinitions columnDefs =
          buildColumnDefs(
              TestColumn.keyColumn(),
              TestColumn.ofUuid("tx_id"),
              TestColumn.ofVarchar("doc_json"),
              TestColumn.ofVarchar("query_text_values['username']"),
              TestColumn.ofDecimal("query_dbl_values['username']"),
              TestColumn.ofBoolean("query_bool_values['username']"),
              TestColumn.ofVarchar("query_null_values['username']"),
              TestColumn.ofDate("query_timestamp_values['username']"));
      List<Row> rows =
          Arrays.asList(
              resultRow(
                  columnDefs,
                  0,
                  byteBufferForKey("doc6"),
                  UUID.randomUUID(),
                  doc6,
                  "user6",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  1,
                  byteBufferForKey("doc4"),
                  UUID.randomUUID(),
                  doc4,
                  "user4",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  2,
                  byteBufferForKey("doc2"),
                  UUID.randomUUID(),
                  doc2,
                  "user2",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  3,
                  byteBufferForKey("doc1"),
                  UUID.randomUUID(),
                  doc1,
                  "user1",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  4,
                  byteBufferForKey("doc3"),
                  UUID.randomUUID(),
                  doc3,
                  "user3",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  5,
                  byteBufferForKey("doc5"),
                  UUID.randomUUID(),
                  doc5,
                  "user5",
                  null,
                  null,
                  null,
                  null));

      AsyncResultSet results = new MockAsyncResultSet(columnDefs, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      FindCollectionOperation operation =
          FindCollectionOperation.sorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              5,
              20,
              CollectionReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindCollectionOperation.OrderBy("username", true)),
              0,
              20,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(5)
          .isEqualTo(
              List.of(
                  objectMapper.readTree(doc1),
                  objectMapper.readTree(doc2),
                  objectMapper.readTree(doc3),
                  objectMapper.readTree(doc4),
                  objectMapper.readTree(doc5)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findAllSortByDate() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['sort_date'], query_dbl_values['sort_date'], query_bool_values['sort_date'], query_null_values['sort_date'], query_timestamp_values['sort_date'] FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

      String doc1 =
          """
                    {
                      "_id": "doc1",
                      "username": "user1",
                      "sort_date": {
                        "$date": 1672531200000
                      }
                    }
                    """;
      String doc2 =
          """
                    {
                      "_id": "doc2",
                      "username": "user2",
                      "sort_date": {
                        "$date": 1672531300000
                      }
                    }
                    """;
      String doc3 =
          """
                    {
                      "_id": "doc3",
                      "username": "user3",
                      "sort_date": {
                        "$date": 1672531400000
                      }
                    }
                    """;
      String doc4 =
          """
                    {
                      "_id": "doc4",
                      "username": "user4"
                      ,
                      "sort_date": {
                        "$date": 1672531500000
                      }
                    }
                    """;
      String doc5 =
          """
                    {
                      "_id": "doc5",
                      "username": "user5",
                      "sort_date": {
                        "$date": 1672531600000
                      }
                    }
                    """;
      String doc6 =
          """
                    {
                      "_id": "doc6",
                      "username": "user6",
                      "sort_date": {
                        "$date": 1672531700000
                      }
                    }
                    """;

      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      ColumnDefinitions columnDefs =
          buildColumnDefs(
              TestColumn.keyColumn(),
              TestColumn.ofUuid("tx_id"),
              TestColumn.ofVarchar("doc_json"),
              TestColumn.ofVarchar("query_text_values['sort_date']"),
              TestColumn.ofDecimal("query_dbl_values['sort_date']"),
              TestColumn.ofBoolean("query_bool_values['sort_date']"),
              TestColumn.ofVarchar("query_null_values['sort_date']"),
              TestColumn.ofDate("query_timestamp_values['sort_date']"));
      List<Row> rows =
          Arrays.asList(
              resultRow(
                  columnDefs,
                  0,
                  byteBufferForKey("doc6"),
                  UUID.randomUUID(),
                  doc6,
                  null,
                  null,
                  null,
                  null,
                  1672531700000L),
              resultRow(
                  columnDefs,
                  1,
                  byteBufferForKey("doc4"),
                  UUID.randomUUID(),
                  doc4,
                  null,
                  null,
                  null,
                  null,
                  1672531500000L),
              resultRow(
                  columnDefs,
                  2,
                  byteBufferForKey("doc2"),
                  UUID.randomUUID(),
                  doc2,
                  null,
                  null,
                  null,
                  null,
                  1672531300000L),
              resultRow(
                  columnDefs,
                  3,
                  byteBufferForKey("doc1"),
                  UUID.randomUUID(),
                  doc1,
                  null,
                  null,
                  null,
                  null,
                  1672531200000L),
              resultRow(
                  columnDefs,
                  4,
                  byteBufferForKey("doc3"),
                  UUID.randomUUID(),
                  doc3,
                  null,
                  null,
                  null,
                  null,
                  1672531400000L),
              resultRow(
                  columnDefs,
                  5,
                  byteBufferForKey("doc5"),
                  UUID.randomUUID(),
                  doc5,
                  null,
                  null,
                  null,
                  null,
                  1672531600000L));

      AsyncResultSet results = new MockAsyncResultSet(columnDefs, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      FindCollectionOperation operation =
          FindCollectionOperation.sorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              5,
              20,
              CollectionReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindCollectionOperation.OrderBy("sort_date", true)),
              0,
              20,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(5)
          .isEqualTo(
              List.of(
                  objectMapper.readTree(doc1),
                  objectMapper.readTree(doc2),
                  objectMapper.readTree(doc3),
                  objectMapper.readTree(doc4),
                  objectMapper.readTree(doc5)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findAllSortWithSkip() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

      String doc1 =
          """
                {
                  "_id": "doc1",
                  "username": "user1"
                }
                """;
      String doc2 =
          """
                {
                  "_id": "doc2",
                  "username": "user2"
                }
                """;
      String doc3 =
          """
                {
                  "_id": "doc3",
                  "username": "user3"
                }
                """;
      String doc4 =
          """
                {
                  "_id": "doc4",
                  "username": "user4"
                }
                """;
      String doc5 =
          """
                {
                  "_id": "doc5",
                  "username": "user5"
                }
                """;
      String doc6 =
          """
                {
                  "_id": "doc6",
                  "username": "user6"
                }
                """;

      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      ColumnDefinitions columnDefs =
          buildColumnDefs(
              TestColumn.keyColumn(),
              TestColumn.ofUuid("tx_id"),
              TestColumn.ofVarchar("doc_json"),
              TestColumn.ofVarchar("query_text_values['username']"),
              TestColumn.ofDecimal("query_dbl_values['username']"),
              TestColumn.ofBoolean("query_bool_values['username']"),
              TestColumn.ofVarchar("query_null_values['username']"),
              TestColumn.ofDate("query_timestamp_values['username']"));
      List<Row> rows =
          Arrays.asList(
              resultRow(
                  columnDefs,
                  0,
                  byteBufferForKey("doc6"),
                  UUID.randomUUID(),
                  doc6,
                  "user6",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  1,
                  byteBufferForKey("doc4"),
                  UUID.randomUUID(),
                  doc4,
                  "user4",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  2,
                  byteBufferForKey("doc2"),
                  UUID.randomUUID(),
                  doc2,
                  "user2",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  3,
                  byteBufferForKey("doc1"),
                  UUID.randomUUID(),
                  doc1,
                  "user1",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  4,
                  byteBufferForKey("doc3"),
                  UUID.randomUUID(),
                  doc3,
                  "user3",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  5,
                  byteBufferForKey("doc5"),
                  UUID.randomUUID(),
                  doc5,
                  "user5",
                  null,
                  null,
                  null,
                  null));

      AsyncResultSet results = new MockAsyncResultSet(columnDefs, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      FindCollectionOperation operation =
          FindCollectionOperation.sorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              5,
              20,
              CollectionReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindCollectionOperation.OrderBy("username", true)),
              5,
              20,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .isEqualTo(List.of(objectMapper.readTree(doc6)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Test
    public void findAllSortDescending() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" LIMIT %s"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

      String doc1 =
          """
                {
                  "_id": "doc1",
                  "username": "user1"
                }
                """;
      String doc2 =
          """
                {
                  "_id": "doc2",
                  "username": "user2"
                }
                """;
      String doc3 =
          """
                {
                  "_id": "doc3",
                  "username": "user3"
                }
                """;
      String doc4 =
          """
                {
                  "_id": "doc4",
                  "username": "user4"
                }
                """;
      String doc5 =
          """
                {
                  "_id": "doc5",
                  "username": "user5"
                }
                """;
      String doc6 =
          """
                {
                  "_id": "doc6",
                  "username": "user6"
                }
                """;

      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      ColumnDefinitions columnDefs =
          buildColumnDefs(
              TestColumn.keyColumn(),
              TestColumn.ofUuid("tx_id"),
              TestColumn.ofVarchar("doc_json"),
              TestColumn.ofVarchar("query_text_values['username']"),
              TestColumn.ofDecimal("query_dbl_values['username']"),
              TestColumn.ofBoolean("query_bool_values['username']"),
              TestColumn.ofVarchar("query_null_values['username']"),
              TestColumn.ofDate("query_timestamp_values['username']"));
      List<Row> rows =
          Arrays.asList(
              resultRow(
                  columnDefs,
                  0,
                  byteBufferForKey("doc6"),
                  UUID.randomUUID(),
                  doc6,
                  "user6",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  1,
                  byteBufferForKey("doc4"),
                  UUID.randomUUID(),
                  doc4,
                  "user4",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  2,
                  byteBufferForKey("doc2"),
                  UUID.randomUUID(),
                  doc2,
                  "user2",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  3,
                  byteBufferForKey("doc1"),
                  UUID.randomUUID(),
                  doc1,
                  "user1",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  4,
                  byteBufferForKey("doc3"),
                  UUID.randomUUID(),
                  doc3,
                  "user3",
                  null,
                  null,
                  null,
                  null),
              resultRow(
                  columnDefs,
                  5,
                  byteBufferForKey("doc5"),
                  UUID.randomUUID(),
                  doc5,
                  "user5",
                  null,
                  null,
                  null,
                  null));

      AsyncResultSet results = new MockAsyncResultSet(columnDefs, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      FindCollectionOperation operation =
          FindCollectionOperation.sorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              5,
              20,
              CollectionReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindCollectionOperation.OrderBy("username", false)),
              0,
              20,
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(5)
          .isEqualTo(
              List.of(
                  objectMapper.readTree(doc6),
                  objectMapper.readTree(doc5),
                  objectMapper.readTree(doc4),
                  objectMapper.readTree(doc3),
                  objectMapper.readTree(doc2)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }
  }

  @Test
  public void findAllSortByUUIDv6() throws Exception {
    // same for uuidv7
    String collectionReadCql =
        "SELECT key, tx_id, doc_json, query_text_values['uuidv6'], query_dbl_values['uuidv6'], query_bool_values['uuidv6'], query_null_values['uuidv6'], query_timestamp_values['uuidv6'] FROM \"%s\".\"%s\" LIMIT %s"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME, 20);

    // These are UUIDv6 ids generated at 30 second intervals.
    String doc1 =
        """
                      {
                        "_id": "doc1",
                        "uuidv6": "1e712685-714f-6720-a23a-c90103f70be6"
                      }
                      """;
    String doc2 =
        """
                      {
                        "_id": "doc2",
                        "uuidv6": "1e712686-8f82-60c0-ac07-7d6641ed230d"
                      }
                      """;
    String doc3 =
        """
                      {
                        "_id": "doc3",
                        "uuidv6": "1e712687-ada3-68f0-93f8-c1ebf8e6fc8c"
                      }
                      """;

    SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
    ColumnDefinitions columnDefs =
        buildColumnDefs(
            TestColumn.keyColumn(),
            TestColumn.ofUuid("tx_id"),
            TestColumn.ofVarchar("doc_json"),
            TestColumn.ofVarchar("query_text_values['uuidv6']"),
            TestColumn.ofDecimal("query_dbl_values['uuidv6']"),
            TestColumn.ofBoolean("query_bool_values['uuidv6']"),
            TestColumn.ofVarchar("query_null_values['uuidv6']"),
            TestColumn.ofDate("query_timestamp_values['uuidv6']"));
    List<Row> rows =
        Arrays.asList(
            resultRow(
                columnDefs,
                1,
                byteBufferForKey("doc1"),
                UUID.randomUUID(),
                doc1,
                "1e712685-714f-6720-a23a-c90103f70be6",
                null,
                null,
                null,
                null),
            resultRow(
                columnDefs,
                2,
                byteBufferForKey("doc2"),
                UUID.randomUUID(),
                doc2,
                "1e712686-8f82-60c0-ac07-7d6641ed230d",
                null,
                null,
                null,
                null),
            resultRow(
                columnDefs,
                3,
                byteBufferForKey("doc3"),
                UUID.randomUUID(),
                doc3,
                "1e712687-ada3-68f0-93f8-c1ebf8e6fc8c",
                null,
                null,
                null,
                null));

    AsyncResultSet results = new MockAsyncResultSet(columnDefs, rows, null);
    final AtomicInteger callCount = new AtomicInteger();
    QueryExecutor queryExecutor = mock(QueryExecutor.class);
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
        .then(
            invocation -> {
              callCount.incrementAndGet();
              return Uni.createFrom().item(results);
            });

    DBLogicalExpression implicitAnd =
        new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
    FindCollectionOperation operation =
        FindCollectionOperation.sorted(
            COMMAND_CONTEXT,
            implicitAnd,
            DocumentProjector.defaultProjector(),
            null,
            5,
            20,
            CollectionReadType.SORTED_DOCUMENT,
            objectMapper,
            List.of(new FindCollectionOperation.OrderBy("uuidv6", true)),
            0,
            20,
            false);

    Supplier<CommandResult> execute =
        operation
            .execute(dataApiRequestInfo, queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    assertThat(callCount.get()).isEqualTo(1);

    // then result
    CommandResult result = execute.get();
    assertThat(result.data().getResponseDocuments())
        .hasSize(3)
        .isEqualTo(
            List.of(
                objectMapper.readTree(doc1),
                objectMapper.readTree(doc2),
                objectMapper.readTree(doc3)));
    assertThat(result.status()).isNullOrEmpty();
    assertThat(result.errors()).isNullOrEmpty();
  }

  @Nested
  class GetVectorDocuments {
    @Test
    public void vectorSearch() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" ORDER BY query_vector_value ANN OF ? LIMIT 2"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
            {
              "_id": "doc1",
              "username": "user1",
              "$vector": [0.25, 0.25, 0.25, 0.25]
            }
            """;
      String doc2 =
          """
            {
              "_id": "doc2",
              "username": "user1",
              "$vector": [0.35, 0.35, 0.35, 0.35]
            }
            """;

      CqlVector<Float> vectorValue = vectorForStatement(0.25f, 0.25f, 0.25f, 0.25f);
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, vectorValue);
      List<Row> rows =
          Arrays.asList(
              resultRow(0, "doc1", UUID.randomUUID(), doc1),
              resultRow(1, "doc2", UUID.randomUUID(), doc2));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeVectorSearch(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);

      FindCollectionOperation operation =
          FindCollectionOperation.vsearch(
              VECTOR_COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.includeAllProjector(),
              null,
              2,
              2,
              CollectionReadType.DOCUMENT,
              objectMapper,
              new float[] {0.25f, 0.25f, 0.25f, 0.25f},
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(2)
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
      assertThat(result.status()).isEmpty();
    }

    @Test
    public void vectorSearchReturnSortVector() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" ORDER BY query_vector_value ANN OF ? LIMIT 2"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                    {
                      "_id": "doc1",
                      "username": "user1",
                      "$vector": [0.25, 0.25, 0.25, 0.25]
                    }
                    """;
      String doc2 =
          """
                    {
                      "_id": "doc2",
                      "username": "user1",
                      "$vector": [0.35, 0.35, 0.35, 0.35]
                    }
                    """;

      CqlVector<Float> vectorValue = vectorForStatement(0.25f, 0.25f, 0.25f, 0.25f);
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, vectorValue);
      List<Row> rows =
          Arrays.asList(
              resultRow(0, "doc1", UUID.randomUUID(), doc1),
              resultRow(1, "doc2", UUID.randomUUID(), doc2));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeVectorSearch(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      FindCollectionOperation operation =
          FindCollectionOperation.vsearch(
              VECTOR_COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.includeAllProjector(),
              null,
              2,
              2,
              CollectionReadType.DOCUMENT,
              objectMapper,
              new float[] {0.25f, 0.25f, 0.25f, 0.25f},
              true);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(2)
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.errors()).isNullOrEmpty();
      assertThat(result.status()).isNotNull();
      assertThat(result.status().get(CommandStatus.SORT_VECTOR))
          .isEqualTo(new float[] {0.25f, 0.25f, 0.25f, 0.25f});
    }

    @Test
    public void vectorSearchWithFilter() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? ORDER BY query_vector_value ANN OF ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                {
                  "_id": "doc1",
                  "username": "user1",
                  "$vector": [0.25, 0.25, 0.25, 0.25]
                }
                """;
      final String textFilterValue = "username " + new DocValueHasher().getHash("user1").hash();
      CqlVector<Float> vectorValue = vectorForStatement(0.25f, 0.25f, 0.25f, 0.25f);
      SimpleStatement stmt =
          SimpleStatement.newInstance(collectionReadCql, textFilterValue, vectorValue);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeVectorSearch(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new TextCollectionFilter("username", MapCollectionFilter.Operator.EQ, "user1"));

      FindCollectionOperation operation =
          FindCollectionOperation.vsearchSingle(
              VECTOR_COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.includeAllProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              new float[] {0.25f, 0.25f, 0.25f, 0.25f},
              false);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      //       assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      //       then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .contains(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }
  }

  @Nested
  class LogicalExpressionOrder {
    @Test
    public void expressionSort() {

      for (int i = 0; i < 20; i++) {
        DBLogicalExpression implicitAnd1 =
            new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
        implicitAnd1.addFilter(new AllCollectionFilter("tags", List.of("tag1", "tag2"), false));
        FindCollectionOperation operation1 =
            FindCollectionOperation.unsortedSingle(
                COMMAND_CONTEXT,
                implicitAnd1,
                DocumentProjector.defaultProjector(),
                CollectionReadType.DOCUMENT,
                objectMapper,
                false);

        List<Expression<BuiltCondition>> expressions1 =
            ExpressionBuilder.buildExpressions(operation1.dbLogicalExpression(), null);

        DBLogicalExpression implicitAnd2 =
            new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
        implicitAnd2.addFilter(new AllCollectionFilter("tags", List.of("tag1", "tag2"), false));

        FindCollectionOperation operation2 =
            FindCollectionOperation.unsortedSingle(
                COMMAND_CONTEXT,
                implicitAnd2,
                DocumentProjector.defaultProjector(),
                CollectionReadType.DOCUMENT,
                objectMapper,
                false);

        List<Expression<BuiltCondition>> expressions2 =
            ExpressionBuilder.buildExpressions(operation2.dbLogicalExpression(), null);
        assertThat(expressions1.toString()).isEqualTo(expressions2.toString());
      }
    }
  }

  @Nested
  class DriverException {
    @Test
    public void readFailureException() throws UnknownHostException {

      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      Node coordinator = mock(Node.class);
      ConsistencyLevel consistencyLevel = ConsistencyLevel.ONE;
      int received = 1;
      int blockFor = 0;
      int numFailures = 1;
      boolean dataPresent = false;
      Map<InetAddress, Integer> reasonMap = new HashMap<>();
      reasonMap.put(InetAddress.getByName("127.0.0.1"), 0x0000);
      Mockito.when(
              queryExecutor.executeVectorSearch(eq(dataApiRequestInfo), any(), any(), anyInt()))
          .thenThrow(
              new ReadFailureException(
                  coordinator,
                  consistencyLevel,
                  received,
                  blockFor,
                  numFailures,
                  dataPresent,
                  reasonMap));

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      FindCollectionOperation operation =
          FindCollectionOperation.vsearch(
              VECTOR_COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              null,
              2,
              2,
              CollectionReadType.DOCUMENT,
              objectMapper,
              new float[] {0.25f, 0.25f, 0.25f, 0.25f},
              false);

      // Throwable
      Throwable failure =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();
      CommandResult.Error error =
          ThrowableToErrorMapper.getMapperWithMessageFunction()
              .apply(failure, failure.getMessage());
      assertThat(error).isNotNull();
      assertThat(error.fields().get("errorCode")).isEqualTo("SERVER_READ_FAILED");
      assertThat(error.fields().get("exceptionClass")).isEqualTo("JsonApiException");
      assertThat(error.httpStatus()).isEqualTo(Response.Status.BAD_GATEWAY);
      assertThat(error.message())
          .startsWith("Database read failed")
          .endsWith(
              "Cassandra failure during read query at consistency ONE (0 responses were required but only 1 replica responded, 1 failed)");
    }
  }

  MockRow resultRow(int index, String key, UUID txId, String doc) {
    return new MockRow(
        KEY_TXID_JSON_COLUMNS,
        index,
        Arrays.asList(byteBufferForKey(key), byteBufferFrom(txId), byteBufferFrom(doc)));
  }

  MockRow resultRow(ColumnDefinitions columnDefs, int index, Object... values) {
    List<ByteBuffer> buffers = Stream.of(values).map(value -> byteBufferFromAny(value)).toList();
    return new MockRow(columnDefs, index, buffers);
  }
}
