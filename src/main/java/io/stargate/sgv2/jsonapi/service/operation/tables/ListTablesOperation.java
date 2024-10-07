package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * List tables operation. Uses {@link CQLSessionCache} to fetch all valid tables for a namespace.
 * The schema check against the table is done in the {@link CollectionTableMatcher} and ignores
 * them.
 *
 * @param explain - returns collection options if `true`; returns only collection names if `false`
 * @param objectMapper {@link ObjectMapper}
 * @param cqlSessionCache {@link CQLSessionCache}
 * @param tableMatcher {@link CollectionTableMatcher}
 * @param commandContext {@link CommandContext}
 */
public record ListTablesOperation(
    boolean explain,
    ObjectMapper objectMapper,
    CQLSessionCache cqlSessionCache,
    CollectionTableMatcher tableMatcher,
    CommandContext<KeyspaceSchemaObject> commandContext)
    implements Operation {

  // shared table matcher instance
  // TODO: if this is static why does the record that have an instance variable passed by the ctor
  // below ?
  private static final CollectionTableMatcher TABLE_MATCHER = new CollectionTableMatcher();

  public ListTablesOperation(
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
            .get(CqlIdentifier.fromInternal(commandContext.schemaObject().name().keyspace()));
    if (keyspaceMetadata == null) {
      return Uni.createFrom()
          .failure(
              ErrorCodeV1.KEYSPACE_DOES_NOT_EXIST.toApiException(
                  "Unknown keyspace '%s', you must create it first",
                  commandContext.schemaObject().name().keyspace()));
    }
    return Uni.createFrom()
        .item(
            () -> {
              List<TableSchemaObject> properties =
                  keyspaceMetadata
                      // get all tables
                      .getTables()
                      .values()
                      .stream()
                      // filter for valid collections
                      .filter(tableMatcher.negate())
                      // map to name
                      .map(table -> TableSchemaObject.getTableSettings(table, objectMapper))
                      // get as list
                      .toList();
              // Wrap the properties list into a command result
              return new Result(explain, properties);
            });
  }

  // simple result wrapper
  private record Result(boolean explain, List<TableSchemaObject> tables)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      if (explain) {
        final List<TableSchemaObject.TableResponse> createCollectionCommands =
            tables().stream()
                .map(tableSchemaObject -> tableSchemaObject.toTableResponse())
                .toList();
        Map<CommandStatus, Object> statuses =
            Map.of(CommandStatus.EXISTING_COLLECTIONS, createCollectionCommands);
        return new CommandResult(statuses);
      } else {
        List<String> tables =
            tables().stream().map(schemaObject -> schemaObject.name().table()).toList();
        Map<CommandStatus, Object> statuses = Map.of(CommandStatus.EXISTING_COLLECTIONS, tables);
        return new CommandResult(statuses);
      }
    }
  }
}
