package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.CountOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CountOperationTest extends OperationTestBase {
  @Nested
  class Execute {
    private final ColumnDefinitions COUNT_RESULT_COLUMNS =
        buildColumnDefs(TestColumn.ofLong("count"));

    @Test
    public void countWithNoFilter() {
      String collectionReadCql =
          "SELECT COUNT(1) AS count FROM \"%s\".\"%s\"".formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      List<Row> rows =
          Arrays.asList(new MockRow(COUNT_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(5L))));
      AsyncResultSet mockResults = new MockAsyncResultSet(COUNT_RESULT_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCount(eq(stmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      CountOperation countOperation = new CountOperation(CONTEXT, LogicalExpression.and());
      Supplier<CommandResult> execute =
          countOperation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isNotNull();
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isEqualTo(5L);
              });
    }

    @Test
    public void countWithDynamic() {
      String collectionReadCql =
          "SELECT COUNT(1) AS count FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final String filterValue = "username " + new DocValueHasher().getHash("user1").hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, filterValue);
      List<Row> rows =
          Arrays.asList(new MockRow(COUNT_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(2))));
      AsyncResultSet mockResults = new MockAsyncResultSet(COUNT_RESULT_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCount(eq(stmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      implicitAnd
          .comparisonExpressions
          .get(0)
          .setDBFilters(
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1")));
      CountOperation countOperation = new CountOperation(CONTEXT, implicitAnd);
      Supplier<CommandResult> execute =
          countOperation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isNotNull();
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isEqualTo(2L);
              });
    }

    @Test
    public void countWithDynamicNoMatch() {
      String collectionReadCql =
          "SELECT COUNT(1) AS count FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final String filterValue = "username " + new DocValueHasher().getHash("user_all").hash();
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, filterValue);
      List<Row> rows =
          Arrays.asList(new MockRow(COUNT_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(0L))));
      AsyncResultSet mockResults = new MockAsyncResultSet(COUNT_RESULT_COLUMNS, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCount(eq(stmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      implicitAnd
          .comparisonExpressions
          .get(0)
          .setDBFilters(
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user_all")));

      CountOperation countOperation = new CountOperation(CONTEXT, implicitAnd);
      Supplier<CommandResult> execute =
          countOperation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isNotNull();
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isEqualTo(0L);
              });
    }

    @Test
    public void error() {
      // failures are propagated down
      RuntimeException failure = new RuntimeException("Ivan fails the test.");
      String collectionReadCql =
          "SELECT COUNT(1) AS count FROM \"%s\".\"%s\"".formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeCount(eq(stmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().failure(failure);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      CountOperation countOperation = new CountOperation(CONTEXT, implicitAnd);
      Throwable result =
          countOperation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      assertThat(result).isEqualTo(failure);
    }
  }
}
