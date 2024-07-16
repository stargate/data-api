package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.collections.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.tables.FindTableOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindOneCommandResolver extends FilterableResolver<FindOneCommand>
    implements CommandResolver<FindOneCommand> {
  private final ObjectMapper objectMapper;
  private final OperationsConfig operationsConfig;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public FindOneCommandResolver(
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    super();
    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;

    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  @Override
  public Class<FindOneCommand> getCommandClass() {
    return FindOneCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, FindOneCommand command) {

    return new FindTableOperation(
        ctx, LogicalExpression.and(), new FindTableOperation.FindTableParams(1));
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindOneCommand command) {

    LogicalExpression logicalExpression = resolve(ctx, command);
    final SortClause sortClause = command.sortClause();
    // validate sort path
    if (sortClause != null) {
      sortClause.validate(ctx);
    }

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);

    FindOneCommand.Options options = command.options();
    boolean includeSimilarity = false;
    boolean includeSortVector = false;
    if (options != null) {
      includeSimilarity = options.includeSimilarity();
      includeSortVector = options.includeSortVector();
    }
    var indexUsage = ctx.schemaObject().newCollectionIndexUsage();
    indexUsage.vectorIndexTag = vector != null;
    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        logicalExpression,
        indexUsage);
    if (vector != null) {
      return FindOperation.vsearchSingle(
          ctx,
          logicalExpression,
          command.buildProjector(includeSimilarity),
          CollectionReadType.DOCUMENT,
          objectMapper,
          vector,
          includeSortVector);
    }

    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindOperation.sortedSingle(
          ctx,
          logicalExpression,
          command.buildProjector(),
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          CollectionReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          0,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          operationsConfig.maxDocumentSortCount(),
          includeSortVector);
    } else {
      return FindOperation.unsortedSingle(
          ctx,
          logicalExpression,
          command.buildProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          includeSortVector);
    }
  }
}
