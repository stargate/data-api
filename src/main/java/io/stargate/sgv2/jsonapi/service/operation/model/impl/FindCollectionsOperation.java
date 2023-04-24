package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.schema.model.JsonapiTableMatcher;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Find collection operation. Uses {@link SchemaManager} to fetch all valid jsonapi tables for a
 * namespace. The schema check against the table is done in the {@link JsonapiTableMatcher}.
 *
 * @param schemaManager {@link SchemaManager}
 * @param tableMatcher {@link JsonapiTableMatcher}
 * @param commandContext {@link CommandContext}
 */
public record FindCollectionsOperation(
    SchemaManager schemaManager, JsonapiTableMatcher tableMatcher, CommandContext commandContext)
    implements Operation {

  // missing keyspace function
  private static final Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>>
      MISSING_KEYSPACE_FUNCTION =
          keyspace -> {
            String message = "Unknown namespace %s, you must create it first.".formatted(keyspace);
            Exception exception = new JsonApiException(ErrorCode.NAMESPACE_DOES_NOT_EXIST, message);
            return Uni.createFrom().failure(exception);
          };

  // shared table matcher instance
  private static final JsonapiTableMatcher TABLE_MATCHER = new JsonapiTableMatcher();

  public FindCollectionsOperation(SchemaManager schemaManager, CommandContext commandContext) {
    this(schemaManager, TABLE_MATCHER, commandContext);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    String namespace = commandContext.namespace();

    // get all valid tables
    // get all tables
    return schemaManager
        .getTables(namespace, MISSING_KEYSPACE_FUNCTION)

        // filter for valid collections
        .filter(tableMatcher)

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
