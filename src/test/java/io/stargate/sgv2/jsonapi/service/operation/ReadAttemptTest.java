package io.stargate.sgv2.jsonapi.service.operation;

import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ReadAttempt}. */
public class ReadAttemptTest {

  private static final TestData TEST_DATA = new TestData();

  @Test
  public void retryOnAllowFilteringError() {

    var msg = "retryOnAllowFilteringError()";
    var mockNode = mock(Node.class);
    var allowFilteringError =
        new InvalidQueryException(
            mockNode,
            "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING");

    var fixture = TEST_DATA.readAttempt().emptyReadAttemptFixture();

    var cql1 =
        "SELECT * FROM %s.%s"
            .formatted(
                TEST_DATA.names.KEYSPACE_NAME.asCql(true), TEST_DATA.names.TABLE_NAME.asCql(true));
    var cql2 =
        "SELECT * FROM %s.%s ALLOW FILTERING"
            .formatted(
                TEST_DATA.names.KEYSPACE_NAME.asCql(true), TEST_DATA.names.TABLE_NAME.asCql(true));

    fixture
        .attempt()
        .setStatus(OperationAttempt.OperationStatus.READY)
        .queryExecutor()
        .doExecuteReadThrowThenReturn(allowFilteringError, fixture.resultSet())
        .attempt()
        .assertCompleted()
        .attempt()
        .assertFailureEmpty(msg)
        .attempt()
        .assertStatus(OperationAttempt.OperationStatus.COMPLETED, msg)
        .attempt()
        .verifyExecuteStatementCalled(2, msg)
        .queryExecutor()
        .verifyExecuteReadCql(msg, cql1, cql2)
        .attempt()
        .verifyOnCompletionCalled(1, msg)
        .attempt()
        .verifyOnCompletionResultSet(fixture.resultSet(), msg)
        .attempt()
        .verifyOnSuccessCalled(1, msg)
        .attempt()
        .verifyOnSuccessResultSet(fixture.resultSet(), msg)
        .attempt()
        .verifyOneWarning(WarningException.Code.QUERY_RETRIED_DUE_TO_INDEXING, msg)
        .attempt()
        .verifyWarningContains("The original query used the CQL: " + cql1, msg)
        .attempt()
        .verifyWarningContains("The original query used the parameters: " + List.of(), msg);
  }
}
