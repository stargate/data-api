package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.DeleteCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/**
 * Resolves the {@link DeleteOneCommand } DeleteOne command implements Filterable to identify the
 * record to be deleted, Based on the filter condition a record will deleted
 */
@ApplicationScoped
public class DeleteOneCommandResolver extends FilterableResolver<DeleteOneCommand>
    implements CommandResolver<DeleteOneCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;

  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public DeleteOneCommandResolver(
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
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, DeleteOneCommand command) {

    FindCollectionOperation findCollectionOperation = getFindOperation(ctx, command);
    return DeleteCollectionOperation.delete(
        ctx, findCollectionOperation, 1, operationsConfig.lwt().retries());
  }

  @Override
  public Class<DeleteOneCommand> getCommandClass() {
    return DeleteOneCommand.class;
  }

  private FindCollectionOperation getFindOperation(
      CommandContext<CollectionSchemaObject> commandContext, DeleteOneCommand command) {

    LogicalExpression logicalExpression = resolve(commandContext, command);
    final SortClause sortClause = command.sortClause();
    // validate sort path
    if (sortClause != null) {
      sortClause.validate(commandContext);
    }

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);
    var indexUsage = commandContext.schemaObject().newCollectionIndexUsage();
    indexUsage.vectorIndexTag = vector != null;

    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        logicalExpression,
        indexUsage);
    if (vector != null) {
      return FindCollectionOperation.vsearchSingle(
          commandContext,
          logicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.KEY,
          objectMapper,
          vector,
          false);
    }

    List<FindCollectionOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindCollectionOperation.sortedSingle(
          commandContext,
          logicalExpression,
          DocumentProjector.includeAllProjector(),
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          CollectionReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          0,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          operationsConfig.maxDocumentSortCount(),
          false);
    } else {
      return FindCollectionOperation.unsortedSingle(
          commandContext,
          logicalExpression,
          DocumentProjector.includeAllProjector(),
          CollectionReadType.KEY,
          objectMapper,
          false);
    }
  }
}