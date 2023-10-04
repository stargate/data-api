package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
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
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link FindOneAndDeleteCommand } */
@ApplicationScoped
public class FindOneAndDeleteCommandResolver extends FilterableResolver<FindOneAndDeleteCommand>
    implements CommandResolver<FindOneAndDeleteCommand> {
  private final Shredder shredder;
  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public FindOneAndDeleteCommandResolver(
      ObjectMapper objectMapper, OperationsConfig operationsConfig, Shredder shredder) {
    super();
    this.objectMapper = objectMapper;
    this.shredder = shredder;
    this.operationsConfig = operationsConfig;
  }

  @Override
  public Class<FindOneAndDeleteCommand> getCommandClass() {
    return FindOneAndDeleteCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, FindOneAndDeleteCommand command) {
    FindOperation findOperation = getFindOperation(commandContext, command);
    final DocumentProjector documentProjector = command.buildProjector();
    if (documentProjector.doIncludeSimilarityScore()) {
      throw new JsonApiException(
          ErrorCode.VECTOR_SEARCH_SIMILARITY_PROJECTION_NOT_SUPPORTED,
          ErrorCode.VECTOR_SEARCH_SIMILARITY_PROJECTION_NOT_SUPPORTED.getMessage());
    }
    // return
    return DeleteOperation.deleteOneAndReturn(
        commandContext, findOperation, operationsConfig.lwt().retries(), documentProjector);
  }

  private FindOperation getFindOperation(
      CommandContext commandContext, FindOneAndDeleteCommand command) {
    //    List<DBFilterBase> filters = resolve(commandContext, command);
    List<DBFilterBase> filters = null;

    final SortClause sortClause = command.sortClause();

    // vectorize sort clause
    commandContext.tryVectorize(objectMapper.getNodeFactory(), sortClause);

    float[] vector = SortClauseUtil.resolveVsearch(sortClause);

    if (vector != null) {
      return FindOperation.vsearchSingle(
          commandContext,
          filters,
          DocumentProjector.identityProjector(),
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
          DocumentProjector.identityProjector(),
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
          filters,
          DocumentProjector.identityProjector(),
          ReadType.DOCUMENT,
          objectMapper);
    }
  }
}
