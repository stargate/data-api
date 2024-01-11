package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
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
public class FindOneCommandResolver extends FilterableResolver<FindOneCommand>
    implements CommandResolver<FindOneCommand> {
  private final ObjectMapper objectMapper;
  private final OperationsConfig operationsConfig;

  @Inject
  public FindOneCommandResolver(ObjectMapper objectMapper, OperationsConfig operationsConfig) {
    super();
    this.objectMapper = objectMapper;
    this.operationsConfig = operationsConfig;
  }

  @Override
  public Class<FindOneCommand> getCommandClass() {
    return FindOneCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindOneCommand command) {
    LogicalExpression logicalExpression = resolve(commandContext, command);
    final SortClause sortClause = command.sortClause();
    sortClause.validate(commandContext.indexingConfig());

    // vectorize sort clause
    commandContext.tryVectorize(objectMapper.getNodeFactory(), sortClause);

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);

    if (vector != null) {
      FindOneCommand.Options options = command.options();
      boolean includeSimilarity = false;
      if (options != null) {
        includeSimilarity = options.includeSimilarity();
      }
      return FindOperation.vsearchSingle(
          commandContext,
          logicalExpression,
          command.buildProjector(includeSimilarity),
          ReadType.DOCUMENT,
          objectMapper,
          vector);
    }

    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindOperation.sortedSingle(
          commandContext,
          logicalExpression,
          command.buildProjector(),
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          operationsConfig.defaultSortPageSize(),
          ReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          0,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          operationsConfig.maxDocumentSortCount());
    } else {
      return FindOperation.unsortedSingle(
          commandContext,
          logicalExpression,
          command.buildProjector(),
          ReadType.DOCUMENT,
          objectMapper);
    }
  }
}
