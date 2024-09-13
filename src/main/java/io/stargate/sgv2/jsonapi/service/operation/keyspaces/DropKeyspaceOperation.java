package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SchemaChangeResult;
import java.util.function.Supplier;

/**
 * Operation that drops a Cassandra keyspace if it exists.
 *
 * @param name Name of the keyspace to drop.
 */
public record DropKeyspaceOperation(String name) implements Operation {

  // simple pattern for the cql
  private static final String DROP_KEYSPACE_CQL = "DROP KEYSPACE IF EXISTS \"%s\";";

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    SimpleStatement deleteStatement =
        SimpleStatement.newInstance(DROP_KEYSPACE_CQL.formatted(name));
    // execute
    return queryExecutor
        .executeDropSchemaChange(dataApiRequestInfo, deleteStatement)

        // if we have a result always respond positively
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }
}
