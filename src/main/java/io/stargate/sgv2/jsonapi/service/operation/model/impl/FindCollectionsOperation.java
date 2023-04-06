package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.schema.CollectionManager;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Find collection operation. Uses {@link CollectionManager} to fetch all valid jsonapi tables for a
 * namespace.
 *
 * @param collectionManager {@link CollectionManager}
 * @param commandContext {@link CommandContext}
 */
public record FindCollectionsOperation(
    CollectionManager collectionManager, CommandContext commandContext) implements Operation {

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    String namespace = commandContext.namespace();

    // get all valid tables
    return collectionManager
        .getValidCollectionTables(namespace)

        // map to name
        .map(Schema.CqlTable::getName)

        // get as list
        .collect()
        .asList()

        // wrap into command result
        .map(Result::new);
  }

  // simple result wrapper
  private record Result(List<String> collections) implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      Map<CommandStatus, Object> statuses = Map.of(CommandStatus.EXISTING_COLLECTIONS, collections);
      return new CommandResult(statuses);
    }
  }
}
