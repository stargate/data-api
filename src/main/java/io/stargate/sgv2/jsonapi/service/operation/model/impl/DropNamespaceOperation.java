package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.function.Supplier;

/**
 * Operation that drops a Cassandra keyspace if it exists.
 *
 * @param name Name of the namespace to drop.
 */
public record DropNamespaceOperation(String name) implements Operation {

  // simple pattern for the cql
  private static final String DROP_KEYSPACE_CQL = "DROP KEYSPACE IF EXISTS \"%s\";";

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    QueryOuterClass.Query query =
        QueryOuterClass.Query.newBuilder().setCql(DROP_KEYSPACE_CQL.formatted(name)).build();

    // execute
    return queryExecutor
        .executeSchemaChange(query)

        // if we have a result always respond positively
        .map(any -> new SchemaChangeResult(true));
  }
}
