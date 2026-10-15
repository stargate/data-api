package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionReadType;
import io.stargate.sgv2.jsonapi.service.operation.collections.FindCollectionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.CollectionFilterResolver;
import io.stargate.sgv2.jsonapi.service.resolver.matcher.FilterResolver;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;

/** Resolves collection and table {@link FindCommand}s. */
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
    int pageSize = operationsConfig.defaultPageSize();
    return resolveCollectionRead(
        commandContext, command, new CollectionReadLimits(pageSize, pageSize, pageSize));
  }

  /**
   * Resolves an internal reranking read. The planner validates user-supplied hybridLimits before
   * building the inner find, so a missing or non-positive limit here is an internal state error.
   */
  public Operation<CollectionSchemaObject> resolveCollectionCandidateCommand(
      CommandContext<CollectionSchemaObject> commandContext, FindCommand command) {
    var options = command.options();
    Preconditions.checkState(
        options != null && options.limit() != null && options.limit() > 0,
        "Candidate reads require an explicit positive options.limit");
    return resolveCollectionRead(
        commandContext,
        command,
        new CollectionReadLimits(
            Math.min(options.limit(), operationsConfig.maxVectorSearchLimit()),
            options.limit(),
            operationsConfig.defaultPageSize()));
  }

  private FindCollectionOperation resolveCollectionRead(
      CommandContext<CollectionSchemaObject> commandContext,
      FindCommand command,
      CollectionReadLimits readLimits) {

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

      return FindCollectionOperation.vsearch(
          commandContext,
          resolvedDbLogicalExpression,
          command.buildProjector(includeSimilarity),
          pageState,
          limit,
          readLimits.vectorPageSize(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          vector,
          includeSortVector);
    }

    // BM25 search / sort?
    SortExpression bm25Expr = SortClauseUtil.resolveBM25Search(sortClause);
    if (bm25Expr != null) {
      return FindCollectionOperation.bm25Multi(
          commandContext,
          resolvedDbLogicalExpression,
          command.buildProjector(),
          pageState,
          limit,
          readLimits.lexicalPageSize(),
          CollectionReadType.DOCUMENT,
          objectMapper,
          bm25Expr);
    }

    List<FindCollectionOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // if orderBy present
    if (orderBy != null) {
      return FindCollectionOperation.sorted(
          commandContext,
          resolvedDbLogicalExpression,
          command.buildProjector(),
          pageState,
          Math.min(limit, readLimits.sortedLimit()),
          // For in-memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          CollectionReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          skip,
          operationsConfig.maxDocumentSortCount(),
          includeSortVector);
    }
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

  /** Concrete execution limits supplied by the caller, independent of query resolution. */
  private record CollectionReadLimits(int vectorPageSize, int lexicalPageSize, int sortedLimit) {}
}
