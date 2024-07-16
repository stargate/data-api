package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindCommandResolver extends FilterableResolver<FindCommand>
    implements CommandResolver<FindCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;
  private final MeterRegistry meterRegistry;
  private final DataApiRequestInfo dataApiRequestInfo;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public FindCommandResolver(
      OperationsConfig operationsConfig,
      ObjectMapper objectMapper,
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
  public Class<FindCommand> getCommandClass() {
    return FindCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, FindCommand command) {
    final LogicalExpression resolvedLogicalExpression = resolve(ctx, command);
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

    // resolve sort clause
    SortClause sortClause = command.sortClause();

    // validate sort path
    if (sortClause != null) {
      sortClause.validate(ctx);
    }

    // if vector search
    float[] vector = SortClauseUtil.resolveVsearch(sortClause);

    addToMetrics(
        meterRegistry,
        dataApiRequestInfo,
        jsonApiMetricsConfig,
        command,
        resolvedLogicalExpression,
        vector != null);

    if (vector != null) {
      limit =
          Math.min(
              limit, operationsConfig.maxVectorSearchLimit()); // Max vector search support is 1000
      return FindOperation.vsearch(
          ctx,
          resolvedLogicalExpression,
          command.buildProjector(includeSimilarity),
          pageState,
          limit,
          operationsConfig.defaultPageSize(),
          ReadType.DOCUMENT,
          objectMapper,
          vector,
          includeSortVector);
    }

    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // if orderBy present
    if (orderBy != null) {
      return FindOperation.sorted(
          ctx,
          resolvedLogicalExpression,
          command.buildProjector(),
          pageState,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          Math.min(limit, operationsConfig.defaultPageSize()),
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          ReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          skip,
          operationsConfig.maxDocumentSortCount(),
          includeSortVector);
    } else {
      return FindOperation.unsorted(
          ctx,
          resolvedLogicalExpression,
          command.buildProjector(),
          pageState,
          limit,
          operationsConfig.defaultPageSize(),
          ReadType.DOCUMENT,
          objectMapper,
          includeSortVector);
    }
  }
}
