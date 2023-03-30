package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindCommandResolver extends FilterableResolver<FindCommand>
    implements CommandResolver<FindCommand> {

  private final DocumentConfig documentConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public FindCommandResolver(DocumentConfig documentConfig, ObjectMapper objectMapper) {
    super();
    this.objectMapper = objectMapper;
    this.documentConfig = documentConfig;
  }

  @Override
  public Class<FindCommand> getCommandClass() {
    return FindCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    final FindCommand.Options options = command.options();
    int limit =
        options != null && options.limit() != null ? options.limit() : documentConfig.maxLimit();
    String pagingState = command.options() != null ? command.options().pagingState() : null;
    final SortClause sortClause = command.sortClause();
    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      int skip =
          options != null && options.skip() != null && options.skip().intValue() > 0
              ? options.skip()
              : 0;
      return FindOperation.sorted(
          commandContext,
          filters,
          command.buildProjector(),
          pagingState,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          Math.min(limit, documentConfig.defaultPageSize()),
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          documentConfig.defaultSortPageSize(),
          ReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          skip,
          documentConfig.maxSortReadLimit());
    } else {
      return FindOperation.unsorted(
          commandContext,
          filters,
          command.buildProjector(),
          pagingState,
          limit,
          documentConfig.defaultPageSize(),
          ReadType.DOCUMENT,
          objectMapper);
    }
  }
}
