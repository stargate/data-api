package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneAndDeleteCommand } */
@ApplicationScoped
public class FindOneAndDeleteCommandResolver extends FilterableResolver<FindOneAndDeleteCommand>
    implements CommandResolver<FindOneAndDeleteCommand> {
  private final Shredder shredder;
  private final DocumentConfig documentConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public FindOneAndDeleteCommandResolver(
      ObjectMapper objectMapper, DocumentConfig documentConfig, Shredder shredder) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.documentConfig = documentConfig;
  }

  @Override
  public Class<FindOneAndDeleteCommand> getCommandClass() {
    return FindOneAndDeleteCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindOneAndDeleteCommand command) {
    FindOperation findOperation = getFindOperation(commandContext, command);
    // return
    return DeleteOperation.deleteOneAndReturn(
        commandContext, findOperation, documentConfig.lwt().retries(), command.buildProjector());
  }

  private FindOperation getFindOperation(
      CommandContext commandContext, FindOneAndDeleteCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    final SortClause sortClause = command.sortClause();
    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindOperation.sorted(
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
      return FindOperation.unsorted(
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
