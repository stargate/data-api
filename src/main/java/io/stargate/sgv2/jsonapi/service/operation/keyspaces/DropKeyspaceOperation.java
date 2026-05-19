package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SchemaChangeResult;
import java.util.function.Supplier;

/**
 * Operation that drops a Cassandra keyspace if it exists.
 *
 * <p>The CQL statement is constructed via the driver's {@link SchemaBuilder} using a {@link
 * CqlIdentifier}, which handles identifier quoting/escaping. This removes the previous {@code
 * String.format} interpolation sink so CQL safety no longer depends on upstream name validation.
 *
 * @param name Name of the keyspace to drop.
 */
public record DropKeyspaceOperation(String name) implements Operation {

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    return queryExecutor
        .executeDropSchemaChange(dataApiRequestInfo, buildStatement())

        // if we have a result always respond positively
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }

  SimpleStatement buildStatement() {
    return SchemaBuilder.dropKeyspace(CqlIdentifier.fromInternal(name)).ifExists().build();
  }
}
