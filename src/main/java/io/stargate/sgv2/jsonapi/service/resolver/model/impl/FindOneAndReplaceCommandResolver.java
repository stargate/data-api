package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadAndUpdateOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv2.jsonapi.util.SortClauseUtil;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/** Resolves the {@link FindOneAndReplaceCommand } */
@ApplicationScoped
public class FindOneAndReplaceCommandResolver extends FilterableResolver<FindOneAndReplaceCommand>
    implements CommandResolver<FindOneAndReplaceCommand> {
  private final Shredder shredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public FindOneAndReplaceCommandResolver(
      ObjectMapper objectMapper, OperationsConfig operationsConfig, Shredder shredder) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.operationsConfig = operationsConfig;
  }

  @Override
  public Class<FindOneAndReplaceCommand> getCommandClass() {
    return FindOneAndReplaceCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindOneAndReplaceCommand command) {
    FindOperation findOperation = getFindOperation(commandContext, command);

    DocumentUpdater documentUpdater = DocumentUpdater.construct(command.replacementDocument());

    // resolve options
    FindOneAndReplaceCommand.Options options = command.options();
    boolean returnUpdatedDocument =
        options != null && "after".equals(command.options().returnDocument());
    boolean upsert = options != null && options.upsert();
    // return
    return new ReadAndUpdateOperation(
        commandContext,
        findOperation,
        documentUpdater,
        true,
        returnUpdatedDocument,
        upsert,
        shredder,
        command.buildProjector(),
        1,
        operationsConfig.lwt().retries());
  }

  private FindOperation getFindOperation(
      CommandContext commandContext, FindOneAndReplaceCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    final SortClause sortClause = command.sortClause();
    List<FindOperation.OrderBy> orderBy = SortClauseUtil.resolveOrderBy(sortClause);
    // If orderBy present
    if (orderBy != null) {
      return FindOperation.sorted(
          commandContext,
          filters,
          DocumentProjector.identityProjector(),
          null,
          1,
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
      return FindOperation.unsorted(
          commandContext,
          filters,
          DocumentProjector.identityProjector(),
          null,
          1,
          1,
          ReadType.DOCUMENT,
          objectMapper);
    }
  }
}
