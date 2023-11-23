package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class FindOperationTest extends OperationTestBase {
  private final CommandContext COMMAND_CONTEXT = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  private final CommandContext VECTOR_COMMAND_CONTEXT =
      new CommandContext(
          KEYSPACE_NAME, COLLECTION_NAME, true, CollectionSettings.SimilarityFunction.COSINE, null);

  private final ColumnDefinitions KEY_TXID_JSON_COLUMNS =
      buildColumnDefs(
          Arrays.asList(
              TestColumn.keyColumn(),
              TestColumn.ofUuid("tx_id"),
              TestColumn.ofVarchar("doc_json")));

  @Inject ObjectMapper objectMapper;

  // !!! Only left temporarily for Disabled tests
  private QueryExecutor queryExecutor0 = mock(QueryExecutor.class);

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
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      List<Row> rows =
          Arrays.asList(
              resultRow(0, "doc1", UUID.randomUUID(), doc1),
              resultRow(1, "doc2", UUID.randomUUID(), doc2));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      FindOperation operation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              20,
              20,
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
      when(queryExecutor.executeRead(eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeRead(eq(stmt2), any(), anyInt()))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.IN,
                  List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"))));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              2,
              2,
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
    public void byIdWithInEmptyArray() {
      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(new DBFilterBase.IDFilter(DBFilterBase.IDFilter.Operator.IN, List.of()));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      FindOperation operation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              2,
              2,
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
      when(queryExecutor.executeRead(eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeRead(eq(stmt2), any(), anyInt()))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));

      List<DBFilterBase> filters1 =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.IN,
                  List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"))));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters1);
      List<DBFilterBase> filters2 =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.TextFilter.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(1).setDBFilters(filters2);

      FindOperation operation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              2,
              2,
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
      when(queryExecutor.executeRead(eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeRead(eq(stmt2), any(), anyInt()))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.IN,
                  List.of(DocumentId.fromString("doc1"), DocumentId.fromString("doc2"))));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              1,
              2,
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);
      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.BoolFilter(
                  "registration_active", DBFilterBase.MapFilterBase.Operator.EQ, true));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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
          "date_field " + new DocValueHasher().getHash(new Date(1672531200000L)).hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, dateFilterValue);
      List<Row> rows = Arrays.asList(resultRow(0, "doc1", UUID.randomUUID(), doc1));
      AsyncResultSet results = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      /*
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      "date_field "
                          + new DocValueHasher().getHash(new Date(1672531200000L)).hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

       */

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.DateFilter(
                  "date_field", DBFilterBase.MapFilterBase.Operator.EQ, new Date(1672531200000L)));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("registration_active"))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(new DBFilterBase.ExistsFilter("registration_active", true));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("tags Stag1"), Values.of("tags Stag2"))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));

      List<DBFilterBase> filters1 =
          List.of(new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters1);
      List<DBFilterBase> filters2 =
          List.of(new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag2"));
      implicitAnd.comparisonExpressions.get(1).setDBFilters(filters2);

      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("tags"), Values.of(2))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters = List.of(new DBFilterBase.SizeFilter("tags", 2));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              1,
              1,
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Disabled
    @Test
    public void findWithArrayEqualFilter() throws Exception {
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
      final String hash = new DocValueHasher().getHash(List.of("tag1", "tag2")).hash();
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("tags"), Values.of(hash))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.ArrayEqualsFilter(
                  new DocValueHasher(), "tags", List.of("tag1", "tag2")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .containsOnly(objectMapper.readTree(doc1));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql, Values.of("sub_doc"), Values.of(hash))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.SubDocEqualsFilter(
                  new DocValueHasher(), "sub_doc", Map.of("col", "val")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

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

    @Disabled
    @Test
    public void failurePropagated() {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      RuntimeException exception = new RuntimeException("Ivan breaks tests.");
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returningFailure(exception);

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      Throwable failure =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(failure).isEqualTo(exception);
    }

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(20)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
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
                          .setType(TypeSpecs.DATE)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc6"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc6),
                          Values.of("user6"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc4"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc4),
                          Values.of("user4"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2),
                          Values.of("user2"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1),
                          Values.of("user1"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc3),
                          Values.of("user3"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc5"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc5),
                          Values.of("user5"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL)));

      LogicalExpression implicitAnd = LogicalExpression.and();
      FindOperation operation =
          FindOperation.sorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              5,
              20,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              0,
              20);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

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

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(20)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_text_values['sort_date']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_dbl_values['sort_date']")
                          .setType(TypeSpecs.DECIMAL)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_bool_values['sort_date']")
                          .setType(TypeSpecs.BOOLEAN)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_null_values['sort_date']")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("query_timestamp_values['sort_date']")
                          .setType(TypeSpecs.DATE)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc6"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc6),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531700000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc4"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc4),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531500000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531300000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531200000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc3),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531400000L)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc5"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc5),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.of(1672531600000L))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      FindOperation operation =
          FindOperation.sorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              5,
              20,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("sort_date", true)),
              0,
              20);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

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

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(20)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
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
                          .setType(TypeSpecs.DATE)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc6"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc6),
                          Values.of("user6"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc4"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc4),
                          Values.of("user4"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2),
                          Values.of("user2"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1),
                          Values.of("user1"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc3),
                          Values.of("user3"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc5"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc5),
                          Values.of("user5"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL)));

      LogicalExpression implicitAnd = LogicalExpression.and();
      FindOperation operation =
          FindOperation.sorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              5,
              20,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              5,
              20);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(1)
          .isEqualTo(List.of(objectMapper.readTree(doc6)));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(20)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
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
                          .setType(TypeSpecs.DATE)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc6"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc6),
                          Values.of("user6"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc4"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc4),
                          Values.of("user4"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2),
                          Values.of("user2"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1),
                          Values.of("user1"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc3"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc3),
                          Values.of("user3"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc5"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc5),
                          Values.of("user5"),
                          Values.NULL,
                          Values.NULL,
                          Values.NULL,
                          Values.NULL)));

      LogicalExpression implicitAnd = LogicalExpression.and();
      FindOperation operation =
          FindOperation.sorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              5,
              20,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", false)),
              0,
              20);

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

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

  @Nested
  class GetDocuments {

    @Disabled
    @Test
    public void findWithId() {
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

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

      ReadOperation.FindResponse result =
          findOperation
              .getDocuments(queryExecutor0, null, null)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result.docs()).isNotNull();
      assertThat(result.docs()).hasSize(1);
    }

    @Disabled
    @Test
    public void findWithIdWithIdRetry() {
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters = List.of(filter);
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      ReadOperation.FindResponse result =
          findOperation
              .getDocuments(queryExecutor0, null, filter)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result.docs()).isNotNull();
      assertThat(result.docs()).hasSize(1);
    }

    @Disabled
    @Test
    public void findWithDynamic() {
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

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
              ReadType.DOCUMENT,
              objectMapper);

      ReadOperation.FindResponse result =
          findOperation
              .getDocuments(queryExecutor0, null, null)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result.docs()).isNotNull();
      assertThat(result.docs()).hasSize(1);
    }

    @Disabled
    @Test
    public void findWithDynamicWithIdRetry() {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      String doc1 =
          """
                        {
                          "_id": "doc1",
                          "username": "user1"
                        }
                        """;
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of(
                      CustomValueSerializers.getDocumentIdValue(DocumentId.fromString("doc1"))),
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

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
              ReadType.DOCUMENT,
              objectMapper);

      DBFilterBase.IDFilter idFilter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));
      ReadOperation.FindResponse result =
          findOperation
              .getDocuments(queryExecutor0, null, idFilter)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result.docs()).isNotNull();
      assertThat(result.docs()).hasSize(1);
    }

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  CustomValueSerializers.getVectorValue(new float[] {0.25f, 0.25f, 0.25f, 0.25f}))
              .withPageSize(2)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1)),
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc2"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc2))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      FindOperation operation =
          FindOperation.vsearch(
              VECTOR_COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              2,
              2,
              ReadType.DOCUMENT,
              objectMapper,
              new float[] {0.25f, 0.25f, 0.25f, 0.25f});

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();
      // then result
      CommandResult result = execute.get();
      assertThat(result.data().getResponseDocuments())
          .hasSize(2)
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
    }

    @Disabled
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
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()),
                  CustomValueSerializers.getVectorValue(new float[] {0.25f, 0.25f, 0.25f, 0.25f}))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("key")
                          .setType(TypeSpecs.tuple(TypeSpecs.TINYINT, TypeSpecs.VARCHAR))
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("tx_id")
                          .setType(TypeSpecs.UUID)
                          .build(),
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("doc_json")
                          .setType(TypeSpecs.VARCHAR)
                          .build()))
              .returning(
                  List.of(
                      List.of(
                          Values.of(
                              CustomValueSerializers.getDocumentIdValue(
                                  DocumentId.fromString("doc1"))),
                          Values.of(UUID.randomUUID()),
                          Values.of(doc1))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation operation =
          FindOperation.vsearchSingle(
              VECTOR_COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper,
              new float[] {0.25f, 0.25f, 0.25f, 0.25f});

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor0)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      //       assert query execution
      candidatesAssert.assertExecuteCount().isOne();

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

    @Disabled
    @Test
    public void expressionSort() {

      for (int i = 0; i < 20; i++) {
        LogicalExpression implicitAnd1 = LogicalExpression.and();
        implicitAnd1.comparisonExpressions.add(new ComparisonExpression(null, null, null));
        implicitAnd1.comparisonExpressions.add(new ComparisonExpression(null, null, null));

        List<DBFilterBase> filters1_1 =
            List.of(new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag1"));
        implicitAnd1.comparisonExpressions.get(0).setDBFilters(filters1_1);
        List<DBFilterBase> filters1_2 =
            List.of(new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag2"));
        implicitAnd1.comparisonExpressions.get(1).setDBFilters(filters1_2);

        FindOperation operation1 =
            FindOperation.unsortedSingle(
                COMMAND_CONTEXT,
                implicitAnd1,
                DocumentProjector.identityProjector(),
                ReadType.DOCUMENT,
                objectMapper);

        List<Expression<BuiltCondition>> expressions1 =
            ExpressionBuilder.buildExpressions(operation1.logicalExpression(), null);

        LogicalExpression implicitAnd2 = LogicalExpression.and();
        implicitAnd2.comparisonExpressions.add(new ComparisonExpression(null, null, null));
        implicitAnd2.comparisonExpressions.add(new ComparisonExpression(null, null, null));

        List<DBFilterBase> filters2_1 =
            List.of(new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag1"));
        implicitAnd2.comparisonExpressions.get(0).setDBFilters(filters2_1);
        List<DBFilterBase> filters2_2 =
            List.of(new DBFilterBase.AllFilter(new DocValueHasher(), "tags", "tag2"));
        implicitAnd2.comparisonExpressions.get(1).setDBFilters(filters2_2);

        FindOperation operation2 =
            FindOperation.unsortedSingle(
                COMMAND_CONTEXT,
                implicitAnd2,
                DocumentProjector.identityProjector(),
                ReadType.DOCUMENT,
                objectMapper);

        List<Expression<BuiltCondition>> expressions2 =
            ExpressionBuilder.buildExpressions(operation2.logicalExpression(), null);
        assertThat(expressions1.toString()).isEqualTo(expressions2.toString());
      }
    }
  }

  MockRow resultRow(int index, String key, UUID txId, String doc) {
    return new MockRow(
        KEY_TXID_JSON_COLUMNS,
        index,
        Arrays.asList(byteBufferForKey(key), byteBufferFrom(txId), byteBufferFrom(doc)));
  }

  // !!! TEMPORARY BOGUS OVERRIDE
  protected ValidatingStargateBridge.QueryExpectation withQuery(Object... args) {
    return null;
  }
}
