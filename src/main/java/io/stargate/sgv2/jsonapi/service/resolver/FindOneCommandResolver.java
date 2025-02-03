package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
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
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final FilterResolver<FindOneCommand, CollectionSchemaObject> collectionFilterResolver;
  private final ReadCommandResolver<FindOneCommand> readCommandResolver;

  @Inject
  public FindOneCommandResolver(
      ObjectMapper objectMapper,
      OperationsConfig operationsConfig,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.readCommandResolver = new ReadCommandResolver<>(objectMapper, operationsConfig);
    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
  }

  @Override
  public Class<FindOneCommand> getCommandClass() {
    return FindOneCommand.class;
  }

  @Override
  public Operation resolveTableCommand(
      CommandContext<TableSchemaObject> ctx, FindOneCommand command) {

    var pageBuilder = ReadAttemptPage.builder().singleResponse(true).mayReturnVector(command);

    // the skip is 0 and the limit is 1 always for findOne
    return readCommandResolver.buildReadOperation(ctx, command, CqlPagingState.EMPTY, pageBuilder);

    // TODO: AARON MAHESH - this is what was here before, leaving until we confirm all good

    //    var attemptBuilder = new TableReadAttemptBuilder(ctx.schemaObject());
    //
    //    // the skip is 0 and the limit is 1 always for findOne. just making that explicit
    //    var commandSkip = 0;
    //    var commandLimit = 1;
    //    attemptBuilder.addBuilderOption(CQLOption.ForSelect.limit(commandLimit));
    //
    //    // work out the CQL order by
    //    var orderByWithWarnings = tableCqlSortClauseResolver.resolve(ctx, command);
    //    attemptBuilder.addOrderBy(orderByWithWarnings);
    //
    //    // and then if we need to do in memory sorting
    //    attemptBuilder.addSorter(
    //        new TableMemorySortClauseResolver<>(
    //          operationsConfig,
    //          orderByWithWarnings.target(),
    //          commandSkip,
    //          commandLimit)
    //        .resolve(ctx, command));
    //
    //    // the columns the user wants
    //    // NOTE: the projection is doing double duty as the select and the doc provider, this
    // projection is still at POC leve
    //    var projection = TableProjection.fromDefinition(objectMapper,
    // command.tableProjectionDefinition(), ctx.schemaObject());
    //    attemptBuilder.addSelect(WithWarnings.of(projection));
    //    attemptBuilder.addDocumentSourceSupplier(projection);
    //
    //    // TODO, we may want the ability to resolve API filter clause into multiple
    //    // dbLogicalExpressions, which will map into multiple readAttempts
    //    var where =
    //        TableWhereCQLClause.forSelect(
    //            ctx.schemaObject(), tableFilterResolver.resolve(ctx, command).target());
    //
    //    var attempts = new OperationAttemptContainer<>(attemptBuilder.build(where));
    //
    //    var pageBuilder =
    //        ReadAttemptPage.<TableSchemaObject>builder()
    //            .singleResponse(true)
    //            .includeSortVector(false)
    //            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
    //            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());
    //
    //    return new GenericOperation<>(attempts, pageBuilder, new TableDriverExceptionHandler());
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> commandContext, FindOneCommand command) {

    final DBLogicalExpression dbLogicalExpression =
        collectionFilterResolver.resolve(commandContext, command).target();
    final SortClause sortClause = command.sortClause();
    if (sortClause != null) {
      sortClause.validate(commandContext.schemaObject());
    }

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);

    FindOneCommand.Options options = command.options();
    boolean includeSimilarity = false;
    boolean includeSortVector = false;
    if (options != null) {
      includeSimilarity = options.includeSimilarity();
      includeSortVector = options.includeSortVector();
    }
    var indexUsage = commandContext.schemaObject().newCollectionIndexUsage();
    indexUsage.vectorIndexTag = vector != null;
    addToMetrics(
        meterRegistry,
        commandContext.requestContext(),
        jsonApiMetricsConfig,
        command,
        dbLogicalExpression,
        indexUsage);
    if (vector != null) {
      return FindCollectionOperation.vsearchSingle(
          commandContext,
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
          commandContext,
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
          commandContext,
          dbLogicalExpression,
          command.buildProjector(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          includeSortVector);
    }
  }
}
