package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.TableFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.SortClauseResolver;
import io.stargate.sgv2.jsonapi.service.resolver.sort.TableSortClauseResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
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

  private final FilterResolver<FindOneCommand, CollectionSchemaObject> collectionFilterResolver;
  private final FilterResolver<FindOneCommand, TableSchemaObject> tableFilterResolver;
  private final SortClauseResolver<FindOneCommand, TableSchemaObject> tableSortClauseResolver;

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
    this.tableSortClauseResolver =
        new TableSortClauseResolver<>(operationsConfig, JSONCodecRegistries.DEFAULT_REGISTRY);
  }

  @Override
  public Class<FindOneCommand> getCommandClass() {
    return FindOneCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, FindOneCommand command) {

    var projection =
        TableRowProjection.fromDefinition(
            objectMapper, command.tableProjectionDefinition(), ctx.schemaObject());

    var orderBy = tableSortClauseResolver.resolve(ctx, command);

    var builder =
        new TableReadAttemptBuilder(ctx.schemaObject(), projection, projection, orderBy)
            .addBuilderOption(CQLOption.ForSelect.limit(1));

    // TODO, we may want the ability to resolve API filter clause into multiple
    // dbLogicalExpressions, which will map into multiple readAttempts
    var where =
        TableWhereCQLClause.forSelect(
            ctx.schemaObject(), tableFilterResolver.resolve(ctx, command).target());

    var attempts = new OperationAttemptContainer<>(builder.build(where));

    var pageBuilder =
        ReadAttemptPage.<TableSchemaObject>builder()
            .singleResponse(true)
            .includeSortVector(false)
            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());

    return new GenericOperation<>(attempts, pageBuilder, new TableDriverExceptionHandler());
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindOneCommand command) {

    final DBLogicalExpression dbLogicalExpression =
        collectionFilterResolver.resolve(ctx, command).target();
    final SortClause sortClause = command.sortClause();
    SchemaValidatable.maybeValidate(ctx, sortClause);

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
        dbLogicalExpression,
        indexUsage);
    if (vector != null) {
      return FindCollectionOperation.vsearchSingle(
          ctx,
          dbLogicalExpression,
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
          dbLogicalExpression,
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
          dbLogicalExpression,
          command.buildProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          includeSortVector);
    }
  }
}
