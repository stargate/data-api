package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Operation that list all available namespaces into the {@link CommandStatus#EXISTING_NAMESPACES}
 * command status.
 *
 * @param schemaManager SGv2 schema manager for keyspace fetching
 */
public record FindNamespacesOperations(SchemaManager schemaManager) implements Operation {

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    // get all existing keyspaces
    return schemaManager
        .getKeyspaces()

        // map to keyspace name
        .map(k -> k.getCqlKeyspace().getName())

        // get as list
        .collect()
        .asList()

        // wrap into command result
        .map(Result::new);
  }

  // simple result wrapper
  private record Result(List<String> keyspaces) implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      Map<CommandStatus, Object> statuses = Map.of(CommandStatus.EXISTING_NAMESPACES, keyspaces);
      return new CommandResult(statuses);
    }
  }
}
