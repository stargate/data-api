package io.stargate.sgv2.jsonapi.service.operation.model.impl;

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
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.api.common.config.QueriesConfig;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteOperationTest extends OperationTestBase {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final CommandContext COMMAND_CONTEXT =
      new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject QueryExecutor queryExecutor;
  @Inject ObjectMapper objectMapper;
  @Inject QueriesConfig queriesConfig;

  @Nested
  class Execute {

    private final ColumnDefinitions DELETE_RESULT_COLUMNS =
        buildColumnDefs(Arrays.asList(TestColumn.ofBoolean("[applied]")));

    private final ColumnDefinitions SELECT_RESULT_COLUMNS =
        buildColumnDefs(Arrays.asList(TestColumn.keyColumn(), TestColumn.ofUuid("tx_id")));

    private final ColumnDefinitions SELECT_WITH_JSON_RESULT_COLUMNS =
        buildColumnDefs(
            Arrays.asList(
                TestColumn.keyColumn(),
                TestColumn.ofUuid("tx_id"),
                TestColumn.ofVarchar("doc_json")));

    @Test
    public void deleteWithId() {
      UUID tx_id = UUID.randomUUID();

      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, keyValue);

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue), byteBufferFrom(tx_id))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);
      assertThat(deleteCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
    }

    @Test
    public void deleteOneAndReturnById() {
      UUID tx_id = UUID.randomUUID();
      String docJson = "{\"_id\":\"doc1\",\"a\":1}";
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, keyValue);

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_WITH_JSON_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(keyValue), byteBufferFrom(tx_id), byteBufferFrom(docJson))));

      AsyncResultSet mockResults =
          new MockAsyncResultSet(SELECT_WITH_JSON_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      DeleteOperation operation =
          DeleteOperation.deleteOneAndReturn(
              COMMAND_CONTEXT, findOperation, 3, DocumentProjector.identityProjector());
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);
      assertThat(deleteCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
      assertThat(result.data().getResponseDocuments()).hasSize(1);
      assertThat(result.data().getResponseDocuments().get(0).toString()).isEqualTo(docJson);
    }

    /*
    @Test
    public void deleteOneAndReturnWithSort() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String docJson1 = "{\"_id\":\"doc1\",\"username\":1,\"status\":\"active\"}";
      String docJson2 = "{\"_id\":\"doc2\",\"username\":2,\"status\":\"active\"}";
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("status " + new DocValueHasher().getHash("active").hash()))
              .withPageSize(2)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_text_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_dbl_values['username']")
                          .setType(TypeSpecs.DECIMAL)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_bool_values['username']")
                          .setType(TypeSpecs.BOOLEAN)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_null_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_timestamp_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1),
                          Values.of(docJson1),
                          Values.NULL,
                          Values.of(new BigDecimal(1)),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id2),
                          Values.of(docJson2),
                          Values.NULL,
                          Values.of(new BigDecimal(2)),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL)));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteFirstAsser =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.sortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              2,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              0,
              3);

      DeleteOperation operation =
          DeleteOperation.deleteOneAndReturn(
              COMMAND_CONTEXT, findOperation, 3, DocumentProjector.identityProjector());

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      candidatesAssert.assertExecuteCount().isEqualTo(2);
      deleteFirstAsser.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
      assertThat(result.data().getResponseDocuments().get(0).toString()).isEqualTo(docJson1);
    }

    @Test
    public void deleteOneAndReturnWithSortDesc() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String docJson1 = "{\"_id\":\"doc1\",\"username\":1,\"status\":\"active\"}";
      String docJson2 = "{\"_id\":\"doc2\",\"username\":2,\"status\":\"active\"}";
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("status " + new DocValueHasher().getHash("active").hash()))
              .withPageSize(2)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_text_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_dbl_values['username']")
                          .setType(TypeSpecs.DECIMAL)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_bool_values['username']")
                          .setType(TypeSpecs.BOOLEAN)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_null_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_timestamp_values['username']")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1),
                          Values.of(docJson1),
                          Values.NULL,
                          Values.of(new BigDecimal(1)),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id2),
                          Values.of(docJson2),
                          Values.NULL,
                          Values.of(new BigDecimal(2)),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL)));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteFirstAsser =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                  Values.of(tx_id2))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.sortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              2,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", false)),
              0,
              3);

      DeleteOperation operation =
          DeleteOperation.deleteOneAndReturn(
              COMMAND_CONTEXT, findOperation, 3, DocumentProjector.identityProjector());

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      candidatesAssert.assertExecuteCount().isEqualTo(2);
      deleteFirstAsser.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
      assertThat(result.data().getResponseDocuments().get(0).toString()).isEqualTo(docJson2);
    }

    @Test
    public void deleteWithIdNoData() {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert readAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(List.of());

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);
      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      readAssert.assertExecuteCount().isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 0);
    }

    @Test
    public void deleteWithDynamic() {
      UUID tx_id = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      deleteAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
    }

    @Test
    public void deleteWithDynamicRetry() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1))));

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert2 =
          withQuery(
                  collectionReadCql2,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id2))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));
      ValidatingStargateBridge.QueryAssert deleteAssert2 =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id2))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 2);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      candidatesAssert2.assertExecuteCount().isOne();
      deleteAssert.assertExecuteCount().isOne();
      deleteAssert2.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
    }

    @Test
    public void deleteWithDynamicRetryFailure() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1))));

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert2 =
          withQuery(
                  collectionReadCql2,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id2))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));
      ValidatingStargateBridge.QueryAssert deleteAssert2 =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id2))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 2);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      candidatesAssert2.assertExecuteCount().isEqualTo(2);
      deleteAssert.assertExecuteCount().isOne();
      deleteAssert2.assertExecuteCount().isEqualTo(2);
      // then result
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(commandResult.errors()).isNotNull();
                assertThat(commandResult.errors()).hasSize(1);
                assertThat(commandResult.errors().get(0).fields().get("errorCode"))
                    .isEqualTo("CONCURRENCY_FAILURE");
                assertThat(commandResult.errors().get(0).message())
                    .isEqualTo(
                        "Failed to delete documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
              });
    }

    @Test
    public void deleteWithDynamicRetryConcurrentDelete() {
      UUID tx_id1 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1))));

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert2 =
          withQuery(
                  collectionReadCql2,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(List.of());

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 2);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      candidatesAssert2.assertExecuteCount().isOne();
      deleteAssert.assertExecuteCount().isOne();
      // then result

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 0);
    }

    @Test
    public void deleteManyWithDynamic() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(2)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id2))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteFirstAsser =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));
      ValidatingStargateBridge.QueryAssert deleteSecondAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                  Values.of(tx_id2))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              2,
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      candidatesAssert.assertExecuteCount().isEqualTo(2);
      deleteFirstAsser.assertExecuteCount().isOne();
      deleteSecondAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 2);
    }

    @Test
    public void deleteManyWithDynamicPaging() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id2))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteFirstAsser =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));
      ValidatingStargateBridge.QueryAssert deleteSecondAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                  Values.of(tx_id2))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              1,
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      candidatesAssert.assertExecuteCount().isEqualTo(3);
      deleteFirstAsser.assertExecuteCount().isOne();
      deleteSecondAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 2);
    }

    @Test
    public void deleteManyWithDynamicPagingAndMoreData() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      UUID tx_id3 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id2)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(tx_id3))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteFirstAsser =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));
      ValidatingStargateBridge.QueryAssert deleteSecondAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                  Values.of(tx_id2))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              1,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isEqualTo(3);
      deleteFirstAsser.assertExecuteCount().isOne();
      deleteSecondAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.DELETED_COUNT, 2)
          .containsEntry(CommandStatus.MORE_DATA, true);
    }

    @Test
    public void deleteWithNoResult() {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(List.of());

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 0);
    }

    @Test
    public void errorPartial() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      UUID tx_id3 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(3)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id2))));

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert2 =
          withQuery(
                  collectionReadCql2,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(3)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id3))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteDoc1Assert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));

      ValidatingStargateBridge.QueryAssert deleteDoc2Assert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                  Values.of(tx_id2))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(true))));

      ValidatingStargateBridge.QueryAssert deleteDoc1RetryAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id3))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              3,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      candidatesAssert2.assertExecuteCount().isEqualTo(3);
      deleteDoc1Assert.assertExecuteCount().isOne();
      deleteDoc1RetryAssert.assertExecuteCount().isEqualTo(3);
      deleteDoc2Assert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();

      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status()).isNotNull();
                assertThat(result.status().get(CommandStatus.DELETED_COUNT)).isEqualTo(1);
                assertThat(result.errors()).isNotNull();
                assertThat(result.errors()).hasSize(1);
                assertThat(result.errors().get(0).fields().get("errorCode"))
                    .isEqualTo("CONCURRENCY_FAILURE");
                assertThat(result.errors().get(0).message())
                    .isEqualTo(
                        "Failed to delete documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
              });
    }

    @Test
    public void errorAll() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      UUID tx_id3 = UUID.randomUUID();
      UUID tx_id4 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(3)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id1)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id2))));

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert candidatesDoc1Assert =
          withQuery(
                  collectionReadCql2,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(3)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(tx_id3))));

      ValidatingStargateBridge.QueryAssert candidatesDoc2Assert =
          withQuery(
                  collectionReadCql2,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(3)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(tx_id4))));

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      ValidatingStargateBridge.QueryAssert deleteDoc1Assert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id1))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));

      ValidatingStargateBridge.QueryAssert deleteDoc2Assert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                  Values.of(tx_id2))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));

      ValidatingStargateBridge.QueryAssert deleteDoc1RetryAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of(tx_id3))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));
      ValidatingStargateBridge.QueryAssert deleteDoc2RetryAssert =
          withQuery(
                  collectionDeleteCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc2"))),
                  Values.of(tx_id4))
              .withSerialConsistency(queriesConfig.serialConsistency())
              .returning(List.of(List.of(Values.of(false))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              3,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      candidatesDoc1Assert.assertExecuteCount().isEqualTo(3);
      deleteDoc1Assert.assertExecuteCount().isOne();
      deleteDoc1RetryAssert.assertExecuteCount().isEqualTo(3);

      candidatesDoc2Assert.assertExecuteCount().isEqualTo(3);
      deleteDoc2Assert.assertExecuteCount().isOne();
      deleteDoc2RetryAssert.assertExecuteCount().isEqualTo(3);

      // then result
      CommandResult result = execute.get();

      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status()).isNotNull();
                assertThat(result.status().get(CommandStatus.DELETED_COUNT)).isEqualTo(0);
                assertThat(result.errors()).isNotNull();
                assertThat(result.errors()).hasSize(1);
                assertThat(result.errors().get(0).fields().get("errorCode"))
                    .isEqualTo("CONCURRENCY_FAILURE");
                assertThat(result.errors().get(0).message())
                    .isEqualTo(
                        "Failed to delete documents with _id ['doc1', 'doc2']: Unable to complete transaction due to concurrent transactions");
              });
    }*/
  }
}
