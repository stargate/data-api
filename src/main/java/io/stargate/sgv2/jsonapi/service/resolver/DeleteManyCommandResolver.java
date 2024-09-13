package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.TruncateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.tables.DeleteTableOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableWhereCQLClause;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Resolves the {@link DeleteManyCommand } DeleteOne command implements Filterable to identify the
 * records to delete based on the filter condition and deletes it.
 */
@ApplicationScoped
public class DeleteManyCommandResolver implements CommandResolver<DeleteManyCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;

  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final FilterResolver<DeleteManyCommand, CollectionSchemaObject> collectionFilterResolver;
  private final FilterResolver<DeleteManyCommand, TableSchemaObject> tableFilterResolver;

  @Inject
  public DeleteManyCommandResolver(
      OperationsConfig operationsConfig,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {

    this.operationsConfig = operationsConfig;
    this.objectMapper = objectMapper;
    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, DeleteManyCommand command) {

    return new DeleteTableOperation(
        ctx,
        TableWhereCQLClause.forDelete(
            ctx.schemaObject(), tableFilterResolver.resolve(ctx, command)));
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, DeleteManyCommand command) {
    // If there is no filter or filter is empty, use Truncate operation instead of Delete
    if (command.filterClause() == null || command.filterClause().logicalExpression().isEmpty()) {
      return new TruncateCollectionOperation(ctx);
    }
    final FindCollectionOperation findCollectionOperation = getFindOperation(ctx, command);
    return DeleteCollectionOperation.delete(
        ctx,
        findCollectionOperation,
        operationsConfig.maxDocumentDeleteCount(),
        operationsConfig.lwt().retries());
  }

  @Override
  public Class<DeleteManyCommand> getCommandClass() {
    return DeleteManyCommand.class;
  }

  private FindCollectionOperation getFindOperation(
      CommandContext<CollectionSchemaObject> ctx, DeleteManyCommand command) {
    final DBLogicalExpression dbLogicalExpression = collectionFilterResolver.resolve(ctx, command);
    // Read One extra document than delete limit so return moreData flag
    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        dbLogicalExpression,
        ctx.schemaObject().newIndexUsage());
    return FindCollectionOperation.unsorted(
        ctx,
        dbLogicalExpression,
        DocumentProjector.includeAllProjector(),
        null,
        operationsConfig.maxDocumentDeleteCount() + 1,
        operationsConfig.defaultPageSize(),
        CollectionReadType.KEY,
        objectMapper,
        false);
  }
}
