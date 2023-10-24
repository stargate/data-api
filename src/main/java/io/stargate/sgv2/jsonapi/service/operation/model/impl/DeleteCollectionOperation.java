package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.function.Supplier;

/**
 * Implementation of the delete collection.
 *
 * @param context Command context, carries namespace of the collection.
 * @param name Collection name.
 */
public record DeleteCollectionOperation(CommandContext context, String name) implements Operation {

  private static final String DROP_TABLE_CQL = "DROP TABLE IF EXISTS \"%s\".\"%s\";";

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    String cql = DROP_TABLE_CQL.formatted(context.namespace(), name);
    SimpleStatement query = SimpleStatement.newInstance(cql);
    // execute
    return queryExecutor
        .executeSchemaChange(query)

        // if we have a result always respond positively
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }
}
