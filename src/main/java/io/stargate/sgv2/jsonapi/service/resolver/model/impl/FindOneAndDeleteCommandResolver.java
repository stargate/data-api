package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneAndDeleteCommand } */
@ApplicationScoped
public class FindOneAndDeleteCommandResolver extends FilterableResolver<FindOneAndDeleteCommand>
    implements CommandResolver<FindOneAndDeleteCommand> {
  private final Shredder shredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public FindOneAndDeleteCommandResolver(
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      Shredder shredder,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.operationsConfig = operationsConfig;

    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  @Override
  public Class<FindOneAndDeleteCommand> getCommandClass() {
    return FindOneAndDeleteCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindOneAndDeleteCommand command) {
    FindOperation findOperation = getFindOperation(ctx, command);
    final DocumentProjector documentProjector = command.buildProjector();
    // return
    return DeleteOperation.deleteOneAndReturn(
        ctx, findOperation, operationsConfig.lwt().retries(), documentProjector);
  }

  private FindOperation getFindOperation(
      CommandContext commandContext, FindOneAndDeleteCommand command) {
    LogicalExpression logicalExpression = resolve(commandContext, command);

    final SortClause sortClause = command.sortClause();
    // validate sort path
    if (sortClause != null) {
      sortClause.validate(commandContext);
    }

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);
    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        logicalExpression,
        vector != null);
    if (vector != null) {
      return FindOperation.vsearchSingle(
          commandContext,
          logicalExpression,
          DocumentProjector.includeAllProjector(),
          ReadType.DOCUMENT,
          objectMapper,
          vector,
          false);
    }

    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindOperation.sortedSingle(
          commandContext,
          logicalExpression,
          DocumentProjector.includeAllProjector(),
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          ReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          0,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          operationsConfig.maxDocumentSortCount(),
          false);
    } else {
      return FindOperation.unsortedSingle(
          commandContext,
          logicalExpression,
          DocumentProjector.includeAllProjector(),
          ReadType.DOCUMENT,
          objectMapper,
          false);
    }
  }
}
