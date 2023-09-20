package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.logging.Log;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
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
    Log.info("??????!");

    List<DBFilterBase> filters = resolve(commandContext, command);
    Log.info("filters after resolve !!!" + filters);
    Log.info("filters after resolve !!!" + filters.size());

    //    for (DBFilterBase filter : filters) {
    //      Log.info("filter after resolve lhs columnName~~~~ " + filter.get().lhs().columnName());
    //      Log.info("filter after resolve lhs value ~~~~ " + filter.get().lhs().value());
    //      Log.info("filter after resolve predicate ~~~~ " + filter.get().predicate());
    //      Log.info("filter after resolve filter value ~~~~ " + filter.get().value().toString());
    //
    //    }
    final SortClause sortClause = command.sortClause();

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);

    if (vector != null) {
      FindOneCommand.Options options = command.options();
      boolean includeSimilarity = false;
      if (options != null) {
        includeSimilarity = options.includeSimilarity();
      }
      return FindOperation.vsearchSingle(
          commandContext,
          filters,
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
          filters,
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
      Log.error("uns 11");
      return FindOperation.unsortedSingle(
          commandContext, filters, command.buildProjector(), ReadType.DOCUMENT, objectMapper);
    }
  }
}
