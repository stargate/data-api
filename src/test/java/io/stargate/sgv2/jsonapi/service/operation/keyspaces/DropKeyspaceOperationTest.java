package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DropKeyspaceOperationTest {

  @Test
  public void buildsExpectedCqlForSimpleName() {
    var op = new DropKeyspaceOperation(identifier("red_star_belgrade"));
    String cql = dropCql(op);
    assertThat(cql).contains("DROP KEYSPACE IF EXISTS").contains("red_star_belgrade");
  }

  @Test
  public void escapesEmbeddedDoubleQuoteInIdentifier() {
    var op = new DropKeyspaceOperation(identifier("foo\"bar"));
    String cql = dropCql(op);
    assertThat(cql).contains("\"foo\"\"bar\"").doesNotContain("\"foo\"bar\"");
  }

  private static CqlIdentifier identifier(String name) {
    return CqlIdentifier.fromInternal(name);
  }

  private static String dropCql(DropKeyspaceOperation op) {
    var requestContext = mock(RequestContext.class);
    var queryExecutor = mock(QueryExecutor.class);
    var resultSet = mock(AsyncResultSet.class);
    when(resultSet.wasApplied()).thenReturn(true);
    when(queryExecutor.executeDropSchemaChange(eq(requestContext), any(SimpleStatement.class)))
        .thenReturn(Uni.createFrom().item(resultSet));

    op.execute(requestContext, queryExecutor).await().indefinitely();

    var statement = ArgumentCaptor.forClass(SimpleStatement.class);
    verify(queryExecutor).executeDropSchemaChange(eq(requestContext), statement.capture());
    return statement.getValue().getQuery();
  }
}
