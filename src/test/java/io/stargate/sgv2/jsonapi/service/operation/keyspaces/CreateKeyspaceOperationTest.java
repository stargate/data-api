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
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Verifies the keyspace CQL passed to the query executor. The operation gets a safe keyspace
 * identifier from the resolver; replication map keys are still validated before this point.
 */
class CreateKeyspaceOperationTest {

  @Nested
  class SimpleStrategy {

    @Test
    public void defaultsToReplicationFactorOne() {
      var op = new CreateKeyspaceOperation(identifier("red_star_belgrade"), null, null);
      String cql = createCql(op);
      assertThat(cql)
          .contains("CREATE KEYSPACE IF NOT EXISTS")
          .contains("red_star_belgrade")
          .contains("'class':'SimpleStrategy'")
          .contains("'replication_factor':1");
    }

    @Test
    public void honoursExplicitReplicationFactor() {
      var op =
          new CreateKeyspaceOperation(
              identifier("red_star_belgrade"), "SimpleStrategy", Map.of("replication_factor", 5));
      String cql = createCql(op);
      assertThat(cql).contains("'class':'SimpleStrategy'").contains("'replication_factor':5");
    }

    @Test
    public void unknownStrategyFallsBackToSimple() {
      var op = new CreateKeyspaceOperation(identifier("k"), "SomeOtherStrategy", null);
      String cql = createCql(op);
      assertThat(cql).contains("'class':'SimpleStrategy'");
    }
  }

  @Nested
  class NetworkTopologyStrategy {

    @Test
    public void supportsRealisticCloudDataCenterNames() {
      var op =
          new CreateKeyspaceOperation(
              identifier("ks"), "NetworkTopologyStrategy", Map.of("us-east-1", 3));
      String cql = createCql(op);
      assertThat(cql).contains("'us-east-1':3");
    }

    @Test
    public void allowsEmptyDataCenterMap() {
      var op = new CreateKeyspaceOperation(identifier("ks"), "NetworkTopologyStrategy", Map.of());
      String cql = createCql(op);
      assertThat(cql).contains("'class':'NetworkTopologyStrategy'");
    }

    @Test
    public void strategyNameIsCaseInsensitive() {
      var op =
          new CreateKeyspaceOperation(
              identifier("ks"), "networktopologystrategy", Map.of("dc1", 3));
      String cql = createCql(op);
      assertThat(cql).contains("'class':'NetworkTopologyStrategy'").contains("'dc1':3");
    }
  }

  @Nested
  class KeyspaceIdentifier {

    @Test
    public void unquotedForSimpleAsciiName() {
      var op = new CreateKeyspaceOperation(identifier("simple_name"), null, null);
      String cql = createCql(op);
      assertThat(cql).contains("CREATE KEYSPACE IF NOT EXISTS simple_name");
    }

    @Test
    public void escapesEmbeddedDoubleQuote() {
      var op = new CreateKeyspaceOperation(identifier("foo\"bar"), null, null);
      String cql = createCql(op);
      assertThat(cql).contains("\"foo\"\"bar\"");
    }
  }

  private static CqlIdentifier identifier(String name) {
    return CqlIdentifier.fromInternal(name);
  }

  private static String createCql(CreateKeyspaceOperation op) {
    var requestContext = mock(RequestContext.class);
    var queryExecutor = mock(QueryExecutor.class);
    var resultSet = mock(AsyncResultSet.class);
    when(resultSet.wasApplied()).thenReturn(true);
    when(queryExecutor.executeCreateSchemaChange(eq(requestContext), any(SimpleStatement.class)))
        .thenReturn(Uni.createFrom().item(resultSet));

    op.execute(requestContext, queryExecutor).await().indefinitely();

    var statement = ArgumentCaptor.forClass(SimpleStatement.class);
    verify(queryExecutor).executeCreateSchemaChange(eq(requestContext), statement.capture());
    return statement.getValue().getQuery();
  }
}
