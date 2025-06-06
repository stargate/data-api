package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
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
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class InsertCollectionOperationTest extends OperationTestBase {

  private TestConstants testConstants = new TestConstants();

  private CommandContext<CollectionSchemaObject> COMMAND_CONTEXT_NON_VECTOR;
  private CommandContext<CollectionSchemaObject> COMMAND_CONTEXT_VECTOR;

  private final ColumnDefinitions COLUMNS_APPLIED =
      buildColumnDefs(TestColumn.ofBoolean("[applied]"));

  private final ColumnDefinitions COLUMNS_APPLIED_FAILURE =
      buildColumnDefs(TestColumn.ofBoolean("[applied]"), TestColumn.ofUuid("tx_id"));

  @Inject DocumentShredder documentShredder;
  @Inject ObjectMapper objectMapper;

  public CollectionInsertAttempt createInsertAttempt(
      CommandContext<CollectionSchemaObject> context, JsonNode document) {
    return createInsertAttempt(context, List.of(document)).getFirst();
  }

  public List<CollectionInsertAttempt> createInsertAttempt(
      CommandContext<CollectionSchemaObject> context, List<JsonNode> documents) {
    var builder =
        new CollectionInsertAttemptBuilder(
            context.schemaObject(), documentShredder, context.commandName());
    return documents.stream().map(builder::build).toList();
  }

  static final String INSERT_CQL =
      "INSERT INTO \"%s\".\"%s\""
          + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values,"
          + " query_dbl_values, query_text_values, query_null_values, query_timestamp_values)"
          + " VALUES"
          + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";

  static final String INSERT_VECTOR_CQL =
      "INSERT INTO \"%s\".\"%s\""
          + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values,"
          + " query_dbl_values, query_text_values, query_null_values, query_timestamp_values, query_vector_value)"
          + " VALUES"
          + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";

  @BeforeEach
  public void beforeEach() {
    super.beforeEach();

    COMMAND_CONTEXT_NON_VECTOR = createCommandContextWithCommandName("testCommand");

    COMMAND_CONTEXT_VECTOR =
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
                            4,
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
  class InsertNonVector {
    @Test
    public void insertOne() throws Exception {
      String document =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"},
                            "date_val" : {"$date": 1672531200000 }
                          }
                          """;

      JsonNode jsonNode = objectMapper.readTree(document);
      var insertAttempt = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode);

      SimpleStatement insertStmt = nonVectorInsertStatement(insertAttempt.document);
      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(COMMAND_CONTEXT_NON_VECTOR, List.of(insertAttempt))
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors()).isEmpty();
    }

    @Test
    public void insertDuplicate() throws Exception {
      String doc1 =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"}
                          }
                          """;

      final JsonNode jsonNode = objectMapper.readTree(doc1);
      var insertAttempt = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode);

      SimpleStatement insertStmt = nonVectorInsertStatement(insertAttempt.document);
      // Note: FALSE is needed to "fail" insertion, producing failure message
      List<Row> rows =
          Arrays.asList(resultRow(COLUMNS_APPLIED_FAILURE, 0, Boolean.FALSE, UUID.randomUUID()));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED_FAILURE, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(COMMAND_CONTEXT_NON_VECTOR, List.of(insertAttempt))
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message()).isEqualTo("Document already exists with the given _id");
                assertThat(error.fields())
                    .containsEntry("exceptionClass", "JsonApiException")
                    .containsEntry("errorCode", "DOCUMENT_ALREADY_EXISTS");
              });
    }

    @Test
    public void insertManyOrdered() throws Exception {
      String document1 =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"}
                          }
                          """;
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      CommandContext commandContext =
          createCommandContextWithCommandName("jsonDocsWrittenInsertManyCommand");

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      var insertAttempt1 = createInsertAttempt(commandContext, jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      var insertAttempt2 = createInsertAttempt(commandContext, jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(insertAttempt1.document);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(insertAttempt2.document);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results1 = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final List<Integer> calls = new ArrayList<>();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                calls.add(1);
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                calls.add(2);
                return Uni.createFrom().item(results2);
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(
                  commandContext, List.of(insertAttempt1, insertAttempt2), true, false, false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(calls).isEqualTo(Arrays.asList(1, 2));

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(
              CommandStatus.INSERTED_IDS,
              List.of(new DocumentId.StringId("doc1"), new DocumentId.StringId("doc2")));
      assertThat(result.errors()).isEmpty();

      // verify metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> jsonDocsWrittenMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written")
                          && !line.startsWith("json_docs_written_bucket")
                          && !line.contains("quantile")
                          && line.contains("jsonDocsWrittenInsertManyCommand"))
              .toList();
      // should have three metrics in total
      assertThat(jsonDocsWrittenMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"jsonDocsWrittenInsertManyCommand\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");
                    });
              });
      // verify count metric -- command called once, the value should be one
      List<String> jsonDocsWrittenCountMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written_count")
                          && line.contains("jsonDocsWrittenInsertManyCommand"))
              .toList();
      assertThat(jsonDocsWrittenCountMetrics).hasSize(1);
      jsonDocsWrittenCountMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(1.0);
          });
      // verify sum metric -- insert two docs, should be two
      List<String> jsonDocsWrittenSumMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written_sum")
                          && line.contains("jsonDocsWrittenInsertManyCommand"))
              .toList();
      assertThat(jsonDocsWrittenSumMetrics).hasSize(1);
      jsonDocsWrittenSumMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(2.0);
          });
    }

    @Test
    public void insertOneRetryLWTCheck() throws Exception {
      String document =
          """
                                  {
                                    "_id": "doc1",
                                    "text": "user1",
                                    "number" : 10,
                                    "boolean": true,
                                    "nullval" : null,
                                    "array" : ["a", "b"],
                                    "sub_doc" : {"col": "val"},
                                    "date_val" : {"$date": 1672531200000 }
                                  }
                                  """;

      JsonNode jsonNode = objectMapper.readTree(document);
      var insertAttempt = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode);

      SimpleStatement insertStmt = nonVectorInsertStatement(insertAttempt.document);
      List<Row> rows =
          Arrays.asList(
              resultRow(
                  COLUMNS_APPLIED_FAILURE, 0, Boolean.FALSE, insertAttempt.document.nextTxID()));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED_FAILURE, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(COMMAND_CONTEXT_NON_VECTOR, List.of(insertAttempt))
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors()).isEmpty();
    }

    @Test
    public void insertManyUnordered() throws Exception {
      String document1 =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"}
                          }
                          """;
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      var insertAttempt1 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      var insertAttempt2 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(insertAttempt1.document);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(insertAttempt2.document);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results1 = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().item(results2);
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(
                  COMMAND_CONTEXT_NON_VECTOR,
                  List.of(insertAttempt1, insertAttempt2),
                  false,
                  false,
                  false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(2);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1);
      assertThat(result.status().get(CommandStatus.INSERTED_IDS))
          .asList()
          .containsExactlyInAnyOrder(
              new DocumentId.StringId("doc1"), new DocumentId.StringId("doc2"));
      assertThat(result.errors()).isEmpty();
    }

    // failure modes

    @Test
    public void failureOrdered() throws Exception {
      // ordered first insert fails
      String document1 =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"}
                          }
                          """;
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      var insertAttempt1 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      var insertAttempt2 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(insertAttempt1.document);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(insertAttempt2.document);

      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Test break #1"));
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().item(Arrays.asList());
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(
                  COMMAND_CONTEXT_NON_VECTOR,
                  List.of(insertAttempt1, insertAttempt2),
                  true,
                  false,
                  false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      // second query never executed
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Server failed: root cause: (java.lang.RuntimeException) Failed to insert document with _id doc1: Test break #1");
                assertThat(error.fields())
                    .containsEntry("errorCode", "SERVER_UNHANDLED_ERROR")
                    .containsEntry("exceptionClass", "JsonApiException");
              });
    }

    @Test
    public void failureOrderedLastFails() throws Exception {
      // ordered first insert OK, second fail
      String document1 =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"}
                          }
                          """;
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      var insertAttempt1 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      var insertAttempt2 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(insertAttempt1.document);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(insertAttempt2.document);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet resultOk = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);

      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().item(resultOk);
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Test break #2"));
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(
                  COMMAND_CONTEXT_NON_VECTOR,
                  List.of(insertAttempt1, insertAttempt2),
                  true,
                  false,
                  false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution: both executed (second failed)
      assertThat(callCount.get()).isEqualTo(2);

      // then result contains both insert and error
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Server failed: root cause: (java.lang.RuntimeException) Failed to insert document with _id doc2: Test break #2");
                assertThat(error.fields())
                    .containsEntry("errorCode", "SERVER_UNHANDLED_ERROR")
                    .containsEntry("exceptionClass", "JsonApiException");
              });
    }

    @Test
    public void failureUnorderedPartial() throws Exception {
      // unordered one query fail
      String document1 =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"}
                          }
                          """;
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      var insertAttempt1 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      var insertAttempt2 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(insertAttempt1.document);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(insertAttempt2.document);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet resultOk = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);

      final AtomicInteger callCount1 = new AtomicInteger();
      final AtomicInteger callCount2 = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount1.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Test break #1"));
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount2.addAndGet(1);
                return Uni.createFrom().item(resultOk);
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(
                  COMMAND_CONTEXT_NON_VECTOR,
                  List.of(insertAttempt1, insertAttempt2),
                  false,
                  false,
                  false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution: both called
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount1.get()).isEqualTo(1);

      // then result has both insert id and errors
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc2")));
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Server failed: root cause: (java.lang.RuntimeException) Failed to insert document with _id doc1: Test break #1");
                assertThat(error.fields())
                    .containsEntry("errorCode", "SERVER_UNHANDLED_ERROR")
                    .containsEntry("exceptionClass", "JsonApiException");
              });
    }

    @Test
    public void failureUnorderedAll() throws Exception {
      // unordered both queries fail
      String document1 =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"}
                          }
                          """;
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      var insertAttempt1 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      var insertAttempt2 = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(insertAttempt1.document);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(insertAttempt2.document);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet resultOk = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);

      final AtomicInteger callCount1 = new AtomicInteger();
      final AtomicInteger callCount2 = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount1.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Insert 1 failed"));
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount2.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Insert 2 failed"));
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(
                  COMMAND_CONTEXT_NON_VECTOR,
                  List.of(insertAttempt1, insertAttempt2),
                  false,
                  false,
                  false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution: both called
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount1.get()).isEqualTo(1);

      // then result has 2 errors
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .hasSize(2)
          .extracting(CommandResult.Error::message)
          .containsExactlyInAnyOrder(
              "Server failed: root cause: (java.lang.RuntimeException) Failed to insert document with _id doc1: Insert 1 failed",
              "Server failed: root cause: (java.lang.RuntimeException) Failed to insert document with _id doc2: Insert 2 failed");
    }
  }

  @Nested
  class InsertVector {
    @Test
    public void insertOneVectorSearch() throws Exception {
      String document =
          """
        {
          "_id": "doc1",
          "text": "user1",
          "number" : 10,
          "boolean": true,
          "nullval" : null,
          "array" : ["a", "b"],
          "sub_doc" : {"col": "val"},
          "date_val" : {"$date": 1672531200000 },
          "$vector" : [0.11,0.22,0.33,0.44]
        }
        """;

      JsonNode jsonNode = objectMapper.readTree(document);
      var insertAttempt = createInsertAttempt(COMMAND_CONTEXT_VECTOR, jsonNode);

      SimpleStatement insertStmt = vectorInsertStatement(insertAttempt.document);
      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(COMMAND_CONTEXT_VECTOR, List.of(insertAttempt))
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors()).isEmpty();
    }

    @Test
    public void insertOneVectorEnabledNoVectorData() throws Exception {
      String document =
          """
        {
          "_id": "doc1",
          "text": "user1",
          "number" : 10,
          "boolean": true,
          "nullval" : null,
          "array" : ["a", "b"],
          "sub_doc" : {"col": "val"},
          "date_val" : {"$date": 1672531200000 }
        }
        """;

      JsonNode jsonNode = objectMapper.readTree(document);
      var insertAttempt = createInsertAttempt(COMMAND_CONTEXT_VECTOR, jsonNode);

      SimpleStatement insertStmt = vectorInsertStatement(insertAttempt.document);
      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertCollectionOperation(COMMAND_CONTEXT_VECTOR, List.of(insertAttempt))
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors()).isEmpty();
    }

    @Test
    public void insertOneVectorDisabledWithVectorData() throws Exception {
      String document =
          """
        {
          "_id": "doc1",
          "text": "user1",
          "number" : 10,
          "boolean": true,
          "nullval" : null,
          "array" : ["a", "b"],
          "sub_doc" : {"col": "val"},
          "date_val" : {"$date": 1672531200000 },
          "$vector" : [0.11,0.22,0.33,0.44]
        }
        """;

      JsonNode jsonNode = objectMapper.readTree(document);
      var insertAttempt = createInsertAttempt(COMMAND_CONTEXT_NON_VECTOR, jsonNode);
      InsertCollectionOperation operation =
          new InsertCollectionOperation(COMMAND_CONTEXT_NON_VECTOR, List.of(insertAttempt));
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      Throwable failure =
          catchThrowable(() -> operation.execute(dataApiRequestInfo, queryExecutor));
      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCodeV1.VECTOR_SEARCH_NOT_SUPPORTED);
    }
  }

  private MockRow resultRow(ColumnDefinitions columnDefs, int index, Object... values) {
    List<ByteBuffer> buffers = Stream.of(values).map(value -> byteBufferFromAny(value)).toList();
    return new MockRow(columnDefs, index, buffers);
  }

  private SimpleStatement nonVectorInsertStatement(WritableShreddedDocument shredDocument) {
    String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    return SimpleStatement.newInstance(
        insertCql,
        CQLBindValues.getDocumentIdValue(shredDocument.id()),
        shredDocument.nextTxID(),
        shredDocument.docJson(),
        CQLBindValues.getSetValue(shredDocument.existKeys()),
        CQLBindValues.getIntegerMapValues(shredDocument.arraySize()),
        shredDocument.arrayContains(),
        CQLBindValues.getBooleanMapValues(shredDocument.queryBoolValues()),
        CQLBindValues.getDoubleMapValues(shredDocument.queryNumberValues()),
        CQLBindValues.getStringMapValues(shredDocument.queryTextValues()),
        CQLBindValues.getSetValue(shredDocument.queryNullValues()),
        CQLBindValues.getTimestampMapValues(shredDocument.queryTimestampValues()));
  }

  private SimpleStatement vectorInsertStatement(WritableShreddedDocument shredDocument) {
    String insertCql = INSERT_VECTOR_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    return SimpleStatement.newInstance(
        insertCql,
        CQLBindValues.getDocumentIdValue(shredDocument.id()),
        shredDocument.nextTxID(),
        shredDocument.docJson(),
        CQLBindValues.getSetValue(shredDocument.existKeys()),
        CQLBindValues.getIntegerMapValues(shredDocument.arraySize()),
        shredDocument.arrayContains(),
        CQLBindValues.getBooleanMapValues(shredDocument.queryBoolValues()),
        CQLBindValues.getDoubleMapValues(shredDocument.queryNumberValues()),
        CQLBindValues.getStringMapValues(shredDocument.queryTextValues()),
        CQLBindValues.getSetValue(shredDocument.queryNullValues()),
        CQLBindValues.getTimestampMapValues(shredDocument.queryTimestampValues()),
        CQLBindValues.getVectorValue(shredDocument.queryVectorValues()));
  }
}
