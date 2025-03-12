package io.stargate.sgv2.jsonapi.service.operation.tasks;

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.operation.ReadDBTask;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ReadDBTask}. */
public class ReadDBTaskTest {

  private static final ReadDBTaskTestData TEST_DATA = new ReadDBTaskTestData();

  @Test
  public void retryOnAllowFilteringError() {

    var msg = "retryOnAllowFilteringError()";
    var mockNode = mock(Node.class);
    var allowFilteringError =
        new InvalidQueryException(
            mockNode,
            "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING");

    var keyspace = "myKeyspace" + System.currentTimeMillis();
    var table = "myTable" + System.currentTimeMillis();
    var task = TEST_DATA.defaultTask(keyspace, table);

    var cql1 =
        "SELECT * FROM %s.%s"
            .formatted(
                CqlIdentifier.fromInternal(keyspace).asCql(true),
                CqlIdentifier.fromInternal(table).asCql(true));
    var cql2 =
        "SELECT * FROM %s.%s ALLOW FILTERING"
            .formatted(
                CqlIdentifier.fromInternal(keyspace).asCql(true),
                CqlIdentifier.fromInternal(table).asCql(true));

    task.doExecuteReadThrowThenReturn(allowFilteringError, task.resultSet)
        .setStatus(Task.TaskStatus.READY)
        .assertCompleted();

    task.verifyExecuteReadCql(msg, cql1, cql2)
        .assertFailureEmpty(msg)
        .assertStatus(Task.TaskStatus.COMPLETED, msg)
        .verifyBuildResultSupplierCalled(2, msg)
        .verifyOnCompletionCalled(1, msg)
        .verifyOnCompletionResult(task.resultSet, msg)
        .verifyOnSuccessCalled(1, msg)
        .verifyOnSuccessResult(task.resultSet, msg)
        .verifyOneWarning(WarningException.Code.QUERY_RETRIED_DUE_TO_INDEXING, msg)
        .verifyWarningContains("The original query used the CQL: " + cql1, msg)
        .verifyWarningContains("The original query used the parameters: " + List.of(), msg);
  }
}
