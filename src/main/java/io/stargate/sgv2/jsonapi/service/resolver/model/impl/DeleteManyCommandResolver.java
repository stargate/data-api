package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.TruncateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Resolves the {@link DeleteManyCommand } DeleteOne command implements Filterable to identify the
 * records to delete based on the filter condition and deletes it.
 */
@ApplicationScoped
public class DeleteManyCommandResolver extends FilterableResolver<DeleteManyCommand>
    implements CommandResolver<DeleteManyCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public DeleteManyCommandResolver(OperationsConfig operationsConfig, ObjectMapper objectMapper) {
    super();
    this.operationsConfig = operationsConfig;
    this.objectMapper = objectMapper;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, DeleteManyCommand command) {
    // if there is no filter or is an empty filter, delete all the rows or truncate the table.
    if (command.filterClause() == null || command.filterClause().logicalExpression().isEmpty()) {
      return new TruncateCollectionOperation(commandContext);
    }
    final FindOperation findOperation = getFindOperation(commandContext, command);
    return DeleteOperation.delete(
        commandContext,
        findOperation,
        operationsConfig.maxDocumentDeleteCount(),
        operationsConfig.lwt().retries());
  }

  @Override
  public Class<DeleteManyCommand> getCommandClass() {
    return DeleteManyCommand.class;
  }

  private FindOperation getFindOperation(CommandContext commandContext, DeleteManyCommand command) {
    LogicalExpression logicalExpression = resolve(commandContext, command);
    // Read One extra document than delete limit so return moreData flag
    return FindOperation.unsorted(
        commandContext,
        logicalExpression,
        DocumentProjector.identityProjector(),
        null,
        operationsConfig.maxDocumentDeleteCount() + 1,
        operationsConfig.defaultPageSize(),
        ReadType.KEY,
        objectMapper);
  }
}
