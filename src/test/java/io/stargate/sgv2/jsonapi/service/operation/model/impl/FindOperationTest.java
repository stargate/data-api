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
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
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
          TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"), TestColumn.ofVarchar("doc_json"));

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
          "date_field " + new DocValueHasher().getHash(Instant.ofEpochMilli(1672531200000L)).hash();
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

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

    @Disabled // fails on binding Collection value
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

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

    @Disabled // fails on binding Map key
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      /*
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

       */

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

    @Disabled // fails on binding Map key
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      /*
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

       */

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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().failure(exception);
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

      Throwable failure =
          operation
              .execute(queryExecutor)
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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

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
      when(queryExecutor.executeRead(eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

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
          .contains(objectMapper.readTree(doc1), objectMapper.readTree(doc2));
      assertThat(result.status()).isNullOrEmpty();
      assertThat(result.errors()).isNullOrEmpty();
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
          FindOperation.vsearchSingle(
              VECTOR_COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper,
              new float[] {0.25f, 0.25f, 0.25f, 0.25f});

      Supplier<CommandResult> execute =
          operation
              .execute(queryExecutor)
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

  MockRow resultRow(ColumnDefinitions columnDefs, int index, Object... values) {
    List<ByteBuffer> buffers = Stream.of(values).map(value -> byteBufferFromAny(value)).toList();
    return new MockRow(columnDefs, index, buffers);
  }
}
