package io.stargate.sgv2.jsonapi.service.operation.tasks;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import java.util.List;

/**
 * TOD: THIS IS NOT NEEDED TO TEST THE BASETASK, NEEDED TO TEST THE DBTASK
 *
 * @param <FixtureT>
 */
public class CommandQueryExecutorAssertions<FixtureT> {

  private final CommandQueryExecutor target;
  private final FixtureT fixture;

  public CommandQueryExecutorAssertions(FixtureT fixture, CommandQueryExecutor target) {
    this.target = target;
    this.fixture = fixture;
  }

  public FixtureT doExecuteReadThrow(Throwable exception) {
    when(target.executeRead(any(SimpleStatement.class))).thenThrow(exception);
    return fixture;
  }

  public FixtureT doExecuteReadThrowThenReturn(Throwable exception, AsyncResultSet resultSet) {

    when(target.executeRead(any(SimpleStatement.class)))
        .thenThrow(exception)
        .thenReturn(Uni.createFrom().item(resultSet));

    return fixture;
  }

  public FixtureT verifyExecuteReadCalled(int times, String message) {
    verify(
            target,
            times(times)
                .description("executeRead() called %s times when: %s".formatted(times, message)))
        .executeRead(any());
    return fixture;
  }

  public FixtureT verifyExecuteReadCql(String msg, String... cql) {

    var captor = forClass(SimpleStatement.class);
    verify(target, times(cql.length)).executeRead(captor.capture());

    List<SimpleStatement> capturedStatements = captor.getAllValues();
    for (int i = 0; i < cql.length; i++) {
      assertThat(capturedStatements.get(i).getQuery())
          .as("CQL Statement for call %s : %s", i, msg)
          .isEqualTo(cql[i]);
    }
    return fixture;
  }
}
