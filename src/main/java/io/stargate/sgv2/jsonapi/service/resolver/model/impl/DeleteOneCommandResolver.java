package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Resolves the {@link DeleteOneCommand } DeleteOne command implements Filterable to identify the
 * record to be deleted, Based on the filter condition a record will deleted
 */
@ApplicationScoped
public class DeleteOneCommandResolver extends FilterableResolver<DeleteOneCommand>
    implements CommandResolver<DeleteOneCommand> {

  @Inject
  public DeleteOneCommandResolver(ObjectMapper objectMapper) {
    super(objectMapper);
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, DeleteOneCommand command) {
    ReadOperation readOperation = resolve(commandContext, command);
    return new DeleteOperation(commandContext, readOperation);
  }

  @Override
  public Class<DeleteOneCommand> getCommandClass() {
    return DeleteOneCommand.class;
  }

  @Override
  protected FilteringOptions getFilteringOption(DeleteOneCommand command) {
    return new FilteringOptions(1, null, 1, FindOperation.ReadType.KEY);
  }
}
