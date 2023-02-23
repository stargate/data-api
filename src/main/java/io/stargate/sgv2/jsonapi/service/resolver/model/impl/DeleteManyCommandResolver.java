package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.service.bridge.config.DocumentConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Resolves the {@link DeleteManyCommand } DeleteOne command implements Filterable to identify the
 * record to be deleted, Based on the filter condition a record will deleted
 */
@ApplicationScoped
public class DeleteManyCommandResolver extends FilterableResolver<DeleteManyCommand>
    implements CommandResolver<DeleteManyCommand> {

  private final DocumentConfig documentConfig;

  @Inject
  public DeleteManyCommandResolver(DocumentConfig documentConfig, ObjectMapper objectMapper) {
    super(objectMapper);
    this.documentConfig = documentConfig;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, DeleteManyCommand command) {
    ReadOperation readOperation = resolve(commandContext, command);
    return new DeleteOperation(commandContext, readOperation);
  }

  @Override
  public Class<DeleteManyCommand> getCommandClass() {
    return DeleteManyCommand.class;
  }

  @Override
  protected FilteringOptions getFilteringOption(DeleteManyCommand command) {
    return new FilteringOptions(
        documentConfig.maxLimit(), null, documentConfig.maxDocumentDeleteCount(), ReadType.KEY);
  }
}
