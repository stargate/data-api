package io.stargate.sgv2.jsonapi.service.operation.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentShredder;
import io.stargate.sgv2.jsonapi.service.shredding.collections.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(SerialConsistencyOverrideOperationTest.SerialConsistencyOverrideProfile.class)
public class SerialConsistencyOverrideOperationTest extends OperationTestBase {
  private CommandContext<CollectionSchemaObject> COMMAND_CONTEXT;
  private final ColumnDefinitions COLUMNS_APPLIED =
      buildColumnDefs(TestColumn.ofBoolean("[applied]"));

  @Inject ObjectMapper objectMapper;
  @Inject DocumentShredder documentShredder;

  @Inject DataVectorizerService dataVectorizerService;

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

  public static class SerialConsistencyOverrideProfile implements QuarkusTestProfile {
    @Override
    public boolean disableGlobalTestResources() {
      return true;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.queries.serial-consistency", "LOCAL_SERIAL")
          .build();
    }
  }

  @BeforeEach
  public void beforeEach() {
    super.beforeEach();
    COMMAND_CONTEXT = createCommandContextWithCommandName("testCommand");
  }

  @Nested
  class Delete {
    @Test
    public void delete() {
      UUID tx_id = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final ColumnDefinitions keyAndTxtIdColumns =
          buildColumnDefs(TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"));
      SimpleStatement selectStmt =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> selectRows =
          Arrays.asList(resultRow(keyAndTxtIdColumns, 0, byteBufferForKey("doc1"), tx_id));
      AsyncResultSet selectResults = new MockAsyncResultSet(keyAndTxtIdColumns, selectRows, null);
      final AtomicInteger callCountSelect = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(selectStmt), any(), anyInt()))
          .then(
              invocation -> {
                callCountSelect.incrementAndGet();
                return Uni.createFrom().item(selectResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, boundKeyForStatement("doc1"), tx_id);
      List<Row> deleteRows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, byteBufferFrom(true)));
      AsyncResultSet deleteResults = new MockAsyncResultSet(COLUMNS_APPLIED, deleteRows, null);
      final AtomicInteger callCountDelete = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                SimpleStatement stmt = invocation.getArgument(1);
                callCountDelete.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(
          new IDCollectionFilter(IDCollectionFilter.Operator.EQ, DocumentId.fromString("doc1")));

      FindCollectionOperation findCollectionOperation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.KEY,
              objectMapper,
              false);
      DeleteCollectionOperation operation =
          DeleteCollectionOperation.delete(COMMAND_CONTEXT, findCollectionOperation, 1, 3);
      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCountSelect.get()).isEqualTo(1);
      assertThat(callCountDelete.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
    }
  }

  @Nested
  class Insert {
    static final String INSERT_CQL =
        "INSERT INTO \"%s\".\"%s\""
            + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values,"
            + " query_dbl_values, query_text_values, query_null_values, query_timestamp_values)"
            + " VALUES"
            + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS";

    @Test
    public void insert() throws Exception {
      String document =
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

      JsonNode jsonNode = objectMapper.readTree(document);
      var insertAttempt = createInsertAttempt(COMMAND_CONTEXT, jsonNode);
      var shredDocument = insertAttempt.document;

      SimpleStatement stmt =
          SimpleStatement.newInstance(
              INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME),
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

      List<Row> resultRows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, byteBufferFrom(true)));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED, resultRows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      InsertCollectionOperation operation =
          new InsertCollectionOperation(COMMAND_CONTEXT, List.of(insertAttempt));
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
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors()).isEmpty();
    }
  }

  @Nested
  class ReadAndUpdate {

    @Test
    public void readAndUpdate() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      UUID tx_id = UUID.randomUUID();
      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1"
                      }
                      """;

      final ColumnDefinitions keyTxIdDocColumns =
          buildColumnDefs(
              TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"), TestColumn.ofVarchar("doc_json"));
      SimpleStatement selectStmt =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> selectRows =
          Arrays.asList(resultRow(keyTxIdDocColumns, 0, byteBufferForKey("doc1"), tx_id, doc1));
      AsyncResultSet selectResults = new MockAsyncResultSet(keyTxIdDocColumns, selectRows, null);
      final AtomicInteger callCountSelect = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(selectStmt), any(), anyInt()))
          .then(
              invocation -> {
                callCountSelect.incrementAndGet();
                return Uni.createFrom().item(selectResults);
              });

      String doc1Updated =
          """
                          {
                            "_id": "doc1",
                            "username": "user1",
                            "name" : "test"
                          }
                          """;

      final String updateCql =
          ReadAndUpdateCollectionOperation.buildUpdateQuery(
              KEYSPACE_NAME, COLLECTION_NAME, false, false);
      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument =
          documentShredder.shred(COMMAND_CONTEXT, jsonNode, tx_id);
      SimpleStatement updateStmt =
          ReadAndUpdateCollectionOperation.bindUpdateValues(updateCql, shredDocument, false, false);

      List<Row> resultRows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, byteBufferFrom(true)));
      AsyncResultSet updateResults = new MockAsyncResultSet(COLUMNS_APPLIED, resultRows, null);
      final AtomicInteger callCountUpdate = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(updateStmt)))
          .then(
              invocation -> {
                callCountUpdate.incrementAndGet();
                return Uni.createFrom().item(updateResults);
              });

      IDCollectionFilter filter =
          new IDCollectionFilter(IDCollectionFilter.Operator.EQ, DocumentId.fromString("doc1"));

      DBLogicalExpression implicitAnd =
          new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
      implicitAnd.addFilter(filter);

      FindCollectionOperation findCollectionOperation =
          FindCollectionOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.defaultProjector(),
              CollectionReadType.DOCUMENT,
              objectMapper,
              false);
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
      ReadAndUpdateCollectionOperation operation =
          new ReadAndUpdateCollectionOperation(
              COMMAND_CONTEXT,
              findCollectionOperation,
              documentUpdater,
              dataVectorizerService,
              true,
              false,
              false,
              documentShredder,
              DocumentProjector.defaultProjector(),
              1,
              3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCountSelect.get()).isEqualTo(1);
      assertThat(callCountUpdate.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
      assertThat(result.errors()).isEmpty();
    }
  }

  private MockRow resultRow(ColumnDefinitions columnDefs, int index, Object... values) {
    List<ByteBuffer> buffers = Stream.of(values).map(value -> byteBufferFromAny(value)).toList();
    return new MockRow(columnDefs, index, buffers);
  }
}
