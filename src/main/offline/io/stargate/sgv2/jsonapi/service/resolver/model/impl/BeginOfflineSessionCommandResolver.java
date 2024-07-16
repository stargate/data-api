package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.BeginOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.BeginOfflineSessionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;

/** Resolves the {@link BeginOfflineSessionCommand}. */
@ApplicationScoped
public class BeginOfflineSessionCommandResolver
    implements CommandResolver<BeginOfflineSessionCommand> {

  @Override
  public Class<BeginOfflineSessionCommand> getCommandClass() {
    return BeginOfflineSessionCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, BeginOfflineSessionCommand command) {
    return new BeginOfflineSessionOperation(command.getSessionId(), command.getFileWriterParams());
  }
}
