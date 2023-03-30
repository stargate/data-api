package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import java.util.List;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneCommand } */
@ApplicationScoped
public class FindOneCommandResolver extends FilterableResolver<FindOneCommand>
    implements CommandResolver<FindOneCommand> {
  private final ObjectMapper objectMapper;
  private final DocumentConfig documentConfig;

  @Inject
  public FindOneCommandResolver(ObjectMapper objectMapper, DocumentConfig documentConfig) {
    super();
    this.objectMapper = objectMapper;
    this.documentConfig = documentConfig;
  }

  @Override
  public Class<FindOneCommand> getCommandClass() {
    return FindOneCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindOneCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    final SortClause sortClause = command.sortClause();
    if (sortClause != null && !sortClause.sortExpressions().isEmpty()) {
      List<FindOperation.OrderBy> orderBy =
          command.sortClause().sortExpressions().stream()
              .map(
                  sortExpression ->
                      new FindOperation.OrderBy(sortExpression.path(), sortExpression.ascending()))
              .collect(Collectors.toList());

      return FindOperation.from(
          commandContext,
          filters,
          // 24-Mar-2023, tatu: Since we update the document, need to avoid modifications on
          // read path, hence pass identity projector.
          DocumentProjector.identityProjector(),
          null,
          1,
          // For in memory sorting we read more data than needed, so defaultSortPageSize like 100
          documentConfig.defaultSortPageSize(),
          ReadType.SORTED_DOCUMENT,
          objectMapper,
          orderBy,
          0,
          // For in memory sorting if no limit provided in the request will use
          // documentConfig.defaultPageSize() as limit
          documentConfig.maxSortReadLimit());
    } else {
      return FindOperation.from(
          commandContext,
          filters,
          // 24-Mar-2023, tatu: Since we update the document, need to avoid modifications on
          // read path, hence pass identity projector.
          DocumentProjector.identityProjector(),
          null,
          1,
          1,
          ReadType.DOCUMENT,
          objectMapper);
    }
  }
}
