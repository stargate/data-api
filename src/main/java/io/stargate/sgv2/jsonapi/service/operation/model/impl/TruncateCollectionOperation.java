package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.function.Supplier;

/**
 * Implementation of the truncate collection.
 *
 * @param context Command context, carries namespace and the name of the collection.
 */
public record TruncateCollectionOperation(CommandContext context) implements Operation {
  private static final String TRUNCATE_TABLE_CQL = "TRUNCATE TABLE \"%s\".\"%s\";";

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    String cql = TRUNCATE_TABLE_CQL.formatted(context.namespace(), context.collection());
    SimpleStatement query = SimpleStatement.newInstance(cql);
    // execute
    return queryExecutor
        .executeSchemaChange(query)

        // if we have a result always respond positively
        .map(any -> new DeleteOperationPage(null, false, false));
  }
}
