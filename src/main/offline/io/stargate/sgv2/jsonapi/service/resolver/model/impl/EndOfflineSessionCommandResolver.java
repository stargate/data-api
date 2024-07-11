package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.EndOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.EndOfflineSessionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;

/** Resolves the {@link EndOfflineSessionCommand}. */
@ApplicationScoped
public class EndOfflineSessionCommandResolver implements CommandResolver<EndOfflineSessionCommand> {

  @Override
  public Class<EndOfflineSessionCommand> getCommandClass() {
    return EndOfflineSessionCommand.class;
  }

  @Override
  public Operation resolveCollectionCommand(
      CommandContext<CollectionSchemaObject> ctx, EndOfflineSessionCommand command) {
    return new EndOfflineSessionOperation(command.sessionId());
  }
}
