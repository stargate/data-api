package io.stargate.sgv2.jsonapi.service.operation.model.collections;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.schema.model.JsonapiTableMatcher;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Find collection operation. Uses {@link CQLSessionCache} to fetch all valid jsonapi tables for a
 * namespace. The schema check against the table is done in the {@link JsonapiTableMatcher}.
 *
 * @param explain - returns collection options if `true`; returns only collection names if `false`
 * @param objectMapper {@link ObjectMapper}
 * @param cqlSessionCache {@link CQLSessionCache}
 * @param tableMatcher {@link JsonapiTableMatcher}
 * @param commandContext {@link CommandContext}
 */
public record FindCollectionsOperation(
    boolean explain,
    ObjectMapper objectMapper,
    CQLSessionCache cqlSessionCache,
    JsonapiTableMatcher tableMatcher,
    CommandContext<KeyspaceSchemaObject> commandContext)
    implements Operation {

  // shared table matcher instance
  // TODO: if this is static why does the record that have an instance variable passed by the ctor
  // below ?
  private static final JsonapiTableMatcher TABLE_MATCHER = new JsonapiTableMatcher();

  public FindCollectionsOperation(
      boolean explain,
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      CommandContext<KeyspaceSchemaObject> commandContext) {
    this(explain, objectMapper, cqlSessionCache, TABLE_MATCHER, commandContext);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    KeyspaceMetadata keyspaceMetadata =
        cqlSessionCache
            .getSession(dataApiRequestInfo)
            .getMetadata()
            .getKeyspaces()
            .get(CqlIdentifier.fromInternal(commandContext.schemaObject().name.keyspace()));
    if (keyspaceMetadata == null) {
      return Uni.createFrom()
          .failure(
              new JsonApiException(
                  ErrorCode.NAMESPACE_DOES_NOT_EXIST,
                  "Unknown namespace %s, you must create it first."
                      .formatted(commandContext.schemaObject().name.keyspace())));
    }
    return Uni.createFrom()
        .item(
            () -> {
              List<CollectionSchemaObject> properties =
                  keyspaceMetadata
                      // get all tables
                      .getTables()
                      .values()
                      .stream()
                      // filter for valid collections
                      .filter(tableMatcher)
                      // map to name
                      .map(
                          table ->
                              CollectionSchemaObject.getCollectionSettings(table, objectMapper))
                      // get as list
                      .toList();
              // Wrap the properties list into a command result
              return new Result(explain, properties);
            });
  }

  // simple result wrapper
  private record Result(boolean explain, List<CollectionSchemaObject> collections)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      if (explain) {
        final List<CreateCollectionCommand> createCollectionCommands =
            collections.stream()
                .map(CollectionSchemaObject::collectionSettingToCreateCollectionCommand)
                .toList();
        Map<CommandStatus, Object> statuses =
            Map.of(CommandStatus.EXISTING_COLLECTIONS, createCollectionCommands);
        return new CommandResult(statuses);
      } else {
        List<String> tables =
            collections.stream().map(schemaObject -> schemaObject.name.table()).toList();
        Map<CommandStatus, Object> statuses = Map.of(CommandStatus.EXISTING_COLLECTIONS, tables);
        return new CommandResult(statuses);
      }
    }
  }
}
