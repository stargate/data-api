package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Resolves the {@link DeleteManyCommand } DeleteOne command implements Filterable to identify the
 * records to delete based on the filter condition and deletes it.
 */
@ApplicationScoped
public class DeleteManyCommandResolver extends FilterableResolver<DeleteManyCommand>
    implements CommandResolver<DeleteManyCommand> {

  private final DocumentConfig documentConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public DeleteManyCommandResolver(DocumentConfig documentConfig, ObjectMapper objectMapper) {
    super();
    this.documentConfig = documentConfig;
    this.objectMapper = objectMapper;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, DeleteManyCommand command) {
    final FindOperation findOperation = getFindOperation(commandContext, command);
    return DeleteOperation.delete(
        commandContext,
        findOperation,
        documentConfig.maxDocumentDeleteCount(),
        documentConfig.lwt().retries());
  }

  @Override
  public Class<DeleteManyCommand> getCommandClass() {
    return DeleteManyCommand.class;
  }

  private FindOperation getFindOperation(CommandContext commandContext, DeleteManyCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    // Read One extra document than delete limit so return moreData flag
    return new FindOperation(
        commandContext,
        filters,
        DocumentProjector.identityProjector(),
        null,
        documentConfig.maxDocumentDeleteCount() + 1,
        documentConfig.defaultPageSize(),
        ReadType.KEY,
        objectMapper,
        null,
        0,
        0);
  }
}
