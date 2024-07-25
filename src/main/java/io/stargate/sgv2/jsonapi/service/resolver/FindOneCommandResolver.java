package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.ValidatableCommandClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.FindTableOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableRowProjection;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindOneCommandResolver implements CommandResolver<FindOneCommand> {
  private final ObjectMapper objectMapper;
  private final OperationsConfig operationsConfig;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final CollectionFilterResolver<FindOneCommand> collectionFilterResolver;
  private final TableFilterResolver<FindOneCommand> tableFilterResolver;

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

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
    this.tableFilterResolver = new TableFilterResolver<>(operationsConfig);
  }

  @Override
  public Class<FindOneCommand> getCommandClass() {
    return FindOneCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, FindOneCommand command) {

    return new FindTableOperation(
        ctx,
        tableFilterResolver.resolve(ctx, command),
        TableRowProjection.fromDefinition(
            objectMapper, command.tableProjectionDefinition(), ctx.schemaObject()),
        new FindTableOperation.FindTableParams(1));
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindOneCommand command) {

    LogicalExpression logicalExpression = collectionFilterResolver.resolve(ctx, command);
    final SortClause sortClause = command.sortClause();
    ValidatableCommandClause.maybeValidate(ctx, sortClause);

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
      return FindCollectionOperation.vsearchSingle(
          ctx,
          logicalExpression,
          command.buildProjector(includeSimilarity),
          CollectionReadType.DOCUMENT,
          objectMapper,
          vector,
          includeSortVector);
    }

    List<FindCollectionOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindCollectionOperation.sortedSingle(
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
      return FindCollectionOperation.unsortedSingle(
          ctx,
          logicalExpression,
          command.buildProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          includeSortVector);
    }
  }
}
