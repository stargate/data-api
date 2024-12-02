package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.processor.SchemaValidatable;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindCommandResolver implements CommandResolver<FindCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final FilterResolver<FindCommand, CollectionSchemaObject> collectionFilterResolver;
  private final ReadCommandResolver<FindCommand> readCommandResolver;

  @Inject
  public FindCommandResolver(
      OperationsConfig operationsConfig,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig) {

    this.readCommandResolver = new ReadCommandResolver<>(objectMapper, operationsConfig);
    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;
    this.meterRegistry = meterRegistry;
    this.dataApiRequestInfo = dataApiRequestInfo;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
  }

  @Override
  public Class<FindCommand> getCommandClass() {
    return FindCommand.class;
  }

  @Override
  public Operation resolveTableCommand(CommandContext<TableSchemaObject> ctx, FindCommand command) {

    // TODO: if we are doing in memory sorting how do we get a paging state working ?
    // The in memory sorting will blank out the paging state so we need to handle this
    var cqlPageState =
        command.options() == null
            ? CqlPagingState.EMPTY
            : CqlPagingState.from(command.options().pageState());

    var pageBuilder =
        ReadAttemptPage.<TableSchemaObject>builder().singleResponse(false).mayReturnVector(command);

    return readCommandResolver.buildReadOperation(ctx, command, cqlPageState, pageBuilder);

    // TODO: AARON MAHESH this is what was here before, leaving until we confirm all good

    //    var rowSorterWithWarnings = tableRowSorterClauseResolver.resolve(ctx, command);
    //
    //    boolean inMemorySort = rowSorterWithWarnings != null;
    //    var operationConfig = ctx.getConfig(OperationsConfig.class);
    //    int limit =
    //        Optional.ofNullable(command.options())
    //            .map(FindCommand.Options::limit)
    //            .orElse(inMemorySort ? operationsConfig.defaultPageSize() : Integer.MAX_VALUE);
    //
    //    var selectLimit = inMemorySort ? operationConfig.maxDocumentSortCount() + 1 : limit;
    //
    //    var cqlPageState =
    //        inMemorySort
    //            ? CqlPagingState.EMPTY
    //            : Optional.ofNullable(command.options())
    //                .map(options -> CqlPagingState.from(options.pageState()))
    //                .orElse(CqlPagingState.EMPTY);
    //
    //    int pageSize =
    //        inMemorySort ? operationsConfig.defaultSortPageSize() :
    // operationsConfig.defaultPageSize();
    //
    //    var orderBy = tableSortOrderByCqlClauseResolver.resolve(ctx, command);
    //
    //    int skip =
    // Optional.ofNullable(command.options()).map(FindCommand.Options::skip).orElse(0);
    //
    //    SortedRowAccumulator.RowSortSettings rowSortSettings =
    //        inMemorySort ? SortedRowAccumulator.RowSortSettings.from(limit, skip, selectLimit) :
    // null;
    //
    //    var projection =
    //        TableProjection.fromDefinition(
    //            objectMapper,
    //            command.tableProjectionDefinition(),
    //            ctx.schemaObject(),
    //            inMemorySort
    //                ?
    // rowSorterWithWarnings.getOrderingColumns().stream().map(ApiColumnDef::name).toList()
    //                : null);
    //
    //    var builder =
    //        new TableReadAttemptBuilder(
    //                ctx.schemaObject(),
    //                projection,
    //                projection,
    //                orderBy,
    //                rowSorterWithWarnings,
    //            rowSortSettings)
    //            .addBuilderOption(CQLOption.ForSelect.limit(selectLimit))
    //            .addStatementOption(CQLOption.ForStatement.pageSize(pageSize))
    //            .addPagingState(cqlPageState);
    //
    //    var where =
    //        TableWhereCQLClause.forSelect(
    //            ctx.schemaObject(), tableFilterResolver.resolve(ctx, command).target());
    //    var attempts = new OperationAttemptContainer<>(builder.build(where));
    //
    //    var pageBuilder =
    //        ReadAttemptPage.<TableSchemaObject>builder()
    //            .singleResponse(false)
    //            .includeSortVector(command.options() != null &&
    // command.options().includeSortVector())
    //            .debugMode(ctx.getConfig(DebugModeConfig.class).enabled())
    //            .useErrorObjectV2(ctx.getConfig(OperationsConfig.class).extendError());
    //
    //    return new GenericOperation<>(attempts, pageBuilder, new TableDriverExceptionHandler());
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindCommand command) {

    var resolvedDbLogicalExpression = collectionFilterResolver.resolve(ctx, command).target();
    // limit and page state defaults
    int limit = Integer.MAX_VALUE;
    int skip = 0;
    String pageState = null;
    boolean includeSimilarity = false;
    boolean includeSortVector = false;

    // update if options provided
    FindCommand.Options options = command.options();
    if (options != null) {
      if (null != options.limit()) {
        limit = options.limit();
      }
      if (null != options.skip()) {
        skip = options.skip();
      }
      pageState = options.pageState();
      includeSimilarity = options.includeSimilarity();
      includeSortVector = options.includeSortVector();
    }

    SortClause sortClause = command.sortClause();

    // collection always uses in memory sorting, so we don't support page state with sort clause
    // empty sort clause and empty page state are treated as no sort clause and no page state
    // any non-zero length string is considered page state - the same standard is used in
    // Tables(CqlPagingState)
    if (sortClause != null && !sortClause.isEmpty() && pageState != null && !pageState.isEmpty()) {
      throw ErrorCodeV1.INVALID_SORT_CLAUSE.toApiException(
          "pageState is not supported with non-empty sort clause");
    }

    SchemaValidatable.maybeValidate(ctx, sortClause);

    // if vector search
    float[] vector = SortClauseUtil.resolveVsearch(sortClause);
    var indexUsage = ctx.schemaObject().newCollectionIndexUsage();
    indexUsage.vectorIndexTag = vector != null;

    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        resolvedDbLogicalExpression,
        indexUsage);

    if (vector != null) {
      limit =
          Math.min(
              limit, operationsConfig.maxVectorSearchLimit()); // Max vector search support is 1000
      return FindCollectionOperation.vsearch(
          ctx,
          resolvedDbLogicalExpression,
          command.buildProjector(includeSimilarity),
          pageState,
          limit,
          operationsConfig.defaultPageSize(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          vector,
          includeSortVector);
    }

    List<FindCollectionOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // if orderBy present
    if (orderBy != null) {
      return FindCollectionOperation.sorted(
          ctx,
          resolvedDbLogicalExpression,
          command.buildProjector(),
          pageState,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          Math.min(limit, operationsConfig.defaultPageSize()),
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          CollectionReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          skip,
          operationsConfig.maxDocumentSortCount(),
          includeSortVector);
    } else {
      return FindCollectionOperation.unsorted(
          ctx,
          resolvedDbLogicalExpression,
          command.buildProjector(),
          pageState,
          limit,
          operationsConfig.defaultPageSize(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          includeSortVector);
    }
  }
}
