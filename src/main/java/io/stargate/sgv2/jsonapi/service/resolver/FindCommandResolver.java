package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.OpenSearchCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindCommandResolver implements CommandResolver<FindCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final FilterResolver<FindCommand, CollectionSchemaObject> collectionFilterResolver;

  @Inject
  public FindCommandResolver(
      OperationsConfig operationsConfig,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig) {

    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;

    this.collectionFilterResolver = new CollectionFilterResolver<>(operationsConfig);
  }

  @Override
  public Class<FindCommand> getCommandClass() {
    return FindCommand.class;
  }

  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, FindCommand command) {

    return new TableReadDBOperationBuilder<>(commandContext)
        .withCommand(command)
        .withPagingState(
            command.options() == null
                ? CqlPagingState.EMPTY
                : CqlPagingState.from(command.options().pageState()))
        .withSingleResponse(false)
        .build();
  }

  @Override
  public Operation<CollectionSchemaObject> resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> commandContext, FindCommand command) {

    var resolvedDbLogicalExpression =
        collectionFilterResolver.resolve(commandContext, command).target();
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

    final SortClause sortClause = command.sortClause(commandContext);

    // collection always uses in memory sorting, so we don't support page state with sort clause
    // empty sort clause and empty page state are treated as no sort clause and no page state
    // any non-zero length string is considered page state - the same standard is used in
    // Tables(CqlPagingState)
    if (sortClause != null) {
      if (!sortClause.isEmpty() && pageState != null && !pageState.isEmpty()) {
        throw SortException.Code.SORT_CLAUSE_INVALID.get(
            Map.of("problem", "'pageState' is not supported with non-empty sort clause"));
      }
      sortClause.validate(commandContext.schemaObject());
    }

    // if vector search
    final float[] vector = SortClauseUtil.resolveVsearch(sortClause);
    var indexUsage = commandContext.schemaObject().newCollectionIndexUsage();
    indexUsage.vectorIndexTag = vector != null;

    addToMetrics(
        meterRegistry,
        commandContext.requestContext(),
        jsonApiMetricsConfig,
        command,
        resolvedDbLogicalExpression,
        indexUsage);

    if (vector != null) {
      limit =
          Math.min(
              limit, operationsConfig.maxVectorSearchLimit()); // Max vector search support is 1000

      // Hack: See https://github.com/stargate/data-api/issues/1961
      int pageSize =
          commandContext.getHybridLimits() == null
              ? operationsConfig.defaultPageSize()
              : commandContext.getHybridLimits().vectorLimit();
      return FindCollectionOperation.vsearch(
          commandContext,
          resolvedDbLogicalExpression,
          command.buildProjector(includeSimilarity),
          pageState,
          limit,
          pageSize,
          CollectionReadType.DOCUMENT,
          objectMapper,
          vector,
          includeSortVector);
    }

    // BM25 search / sort?
    SortExpression bm25Expr = SortClauseUtil.resolveBM25Search(sortClause);
    if (bm25Expr != null) {
      // Hack: See https://github.com/stargate/data-api/issues/1961
      int pageSize =
          commandContext.getHybridLimits() == null
              ? operationsConfig.defaultPageSize()
              : commandContext.getHybridLimits().lexicalLimit();
      return FindCollectionOperation.bm25Multi(
          commandContext,
          resolvedDbLogicalExpression,
          command.buildProjector(),
          pageState,
          limit,
          pageSize,
          CollectionReadType.DOCUMENT,
          objectMapper,
          bm25Expr);
    }

    List<FindCollectionOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // if orderBy present
    if (orderBy != null) {
      // Hack: See https://github.com/stargate/data-api/issues/1961
      // not commandContext.getHybridLimits() becuase there is no limit for a non ANN or BM25 query
      return FindCollectionOperation.sorted(
          commandContext,
          resolvedDbLogicalExpression,
          command.buildProjector(),
          pageState,
          // For in-memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          Math.min(limit, operationsConfig.defaultPageSize()),
          // For in-memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          CollectionReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          skip,
          operationsConfig.maxDocumentSortCount(),
          includeSortVector);
    }

    // OpenSearch $search: the expr(index, ?) predicate is fully evaluated inside OpenSearch before
    // Cassandra returns rows — a CQL LIMIT N would cap the result to N rows in a single shot and
    // cause hasMorePages()=false, so there would be no cursor for page 2.
    // Fix: emit NO CQL LIMIT (buildSelectQueries will pass null to QueryBuilder when it detects an
    // OpenSearch filter); the driver's pageSize controls how many rows come back per Cassandra
    // page,
    // which gives us a real pagingState cursor for subsequent pages.
    // The in-memory limit trim (applyLimitToFindResponses) is bypassed via Integer.MAX_VALUE.
    if (hasOpenSearchFilter(resolvedDbLogicalExpression)) {
      int openSearchPageSize =
          (limit == Integer.MAX_VALUE)
              ? QueryBuilder.DEFAULT_BM25_LIMIT
              : Math.min(limit, QueryBuilder.MAX_BM25_LIMIT);
      return FindCollectionOperation.unsorted(
          commandContext,
          resolvedDbLogicalExpression,
          command.buildProjector(),
          pageState,
          Integer.MAX_VALUE, // no in-memory trim — OpenSearch already bounded the result set
          openSearchPageSize, // driver pageSize controls the Cassandra fetch window
          CollectionReadType.DOCUMENT,
          objectMapper,
          includeSortVector);
    }

    // Hack: See https://github.com/stargate/data-api/issues/1961
    // not commandContext.getHybridLimits() becuase there is no limit for a non ANN or BM25 query
    return FindCollectionOperation.unsorted(
        commandContext,
        resolvedDbLogicalExpression,
        command.buildProjector(),
        pageState,
        limit,
        operationsConfig.defaultPageSize(),
        CollectionReadType.DOCUMENT,
        objectMapper,
        includeSortVector);
  }

  /**
   * Returns true if the resolved expression contains at least one {@link
   * OpenSearchCollectionFilter}.
   */
  private static boolean hasOpenSearchFilter(DBLogicalExpression expr) {
    for (var filter : expr.filters()) {
      if (filter instanceof OpenSearchCollectionFilter) {
        return true;
      }
    }
    for (var sub : expr.subExpressions()) {
      if (hasOpenSearchFilter(sub)) {
        return true;
      }
    }
    return false;
  }
}
