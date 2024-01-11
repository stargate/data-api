package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
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

  @Inject
  public FindCommandResolver(OperationsConfig operationsConfig, ObjectMapper objectMapper) {
    super();
    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;
  }

  @Override
  public Class<FindCommand> getCommandClass() {
    return FindCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindCommand command) {
    final LogicalExpression resolvedLogicalExpression = resolve(commandContext, command);
    // limit and page state defaults
    int limit = Integer.MAX_VALUE;
    int skip = 0;
    String pageState = null;
    boolean includeSimilarity = false;

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
    }

    // resolve sort clause
    SortClause sortClause = command.sortClause();

    // validate sort path
    if (sortClause != null) {
      sortClause.validate(commandContext.collectionSettings().indexingConfig());
    }

    // vectorize sort clause
    commandContext.tryVectorize(objectMapper.getNodeFactory(), sortClause);

    // if vector search
    float[] vector = SortClauseUtil.resolveVsearch(sortClause);

    if (vector != null) {
      limit =
          Math.min(
              limit, operationsConfig.maxVectorSearchLimit()); // Max vector search support is 1000
      return FindOperation.vsearch(
          commandContext,
          resolvedLogicalExpression,
          command.buildProjector(includeSimilarity),
          pageState,
          limit,
          operationsConfig.defaultPageSize(),
          ReadType.DOCUMENT,
          objectMapper,
          vector);
    }

    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // if orderBy present
    if (orderBy != null) {
      return FindOperation.sorted(
          commandContext,
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
          operationsConfig.maxDocumentSortCount());
    } else {
      return FindOperation.unsorted(
          commandContext,
          resolvedLogicalExpression,
          command.buildProjector(),
          pageState,
          limit,
          operationsConfig.defaultPageSize(),
          ReadType.DOCUMENT,
          objectMapper);
    }
  }
}
