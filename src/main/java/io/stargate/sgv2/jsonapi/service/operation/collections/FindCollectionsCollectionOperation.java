package io.stargate.sgv2.jsonapi.service.operation.collections;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionTableMatcher;
import java.util.List;
import java.util.function.Supplier;

/**
 * Find collection operation. Uses {@link CQLSessionCache} to fetch all valid jsonapi tables for a
 * namespace. The schema check against the table is done in the {@link CollectionTableMatcher}.
 *
 * @param explain - returns collection options if `true`; returns only collection names if `false`
 * @param objectMapper {@link ObjectMapper}
 * @param cqlSessionCache {@link CQLSessionCache}
 * @param tableMatcher {@link CollectionTableMatcher}
 * @param commandContext {@link CommandContext}
 */
public record FindCollectionsCollectionOperation(
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

  public FindCollectionsCollectionOperation(
      boolean explain,
      ObjectMapper objectMapper,
      CQLSessionCache cqlSessionCache,
      CommandContext<KeyspaceSchemaObject> commandContext) {
    this(explain, objectMapper, cqlSessionCache, TABLE_MATCHER, commandContext);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {

    return queryExecutor
        .getDriverMetadata(requestContext)
        .map(Metadata::getKeyspaces)
        .map(keyspaces -> keyspaces.get(commandContext.schemaObject().identifier().keyspace()))
        .map(
            keyspaceMetadata -> {
              if (keyspaceMetadata == null) {
                throw SchemaException.Code.UNKNOWN_KEYSPACE.get(
                    errVars(commandContext.schemaObject()));
              }
              var collections =
                  keyspaceMetadata.getTables().values().stream()
                      .filter(tableMatcher)
                      .map(
                          table ->
                              CollectionSchemaObject.getCollectionSettings(
                                  requestContext.tenant(), table, objectMapper))
                      .toList();
              return new Result(explain, collections);
            });
  }

  // simple result wrapper
  private record Result(boolean explain, List<CollectionSchemaObject> collections)
      implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {

      var builder = CommandResult.statusOnlyBuilder(false, false, RequestTracing.NO_OP);
      if (explain) {
        final List<CreateCollectionCommand> createCollectionCommands =
            collections.stream()
                .map(CollectionSchemaObject::collectionSettingToCreateCollectionCommand)
                .toList();
        builder.addStatus(CommandStatus.EXISTING_COLLECTIONS, createCollectionCommands);
      } else {
        List<String> tables =
            collections.stream()
                .map(
                    schemaObject -> cqlIdentifierToMessageString(schemaObject.identifier().table()))
                .toList();
        builder.addStatus(CommandStatus.EXISTING_COLLECTIONS, tables);
      }
      return builder.build();
    }
  }
}
