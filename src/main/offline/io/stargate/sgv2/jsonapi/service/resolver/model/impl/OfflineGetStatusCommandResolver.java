package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineGetStatusCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.OfflineGetStatusOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;

/** Resolves the {@link OfflineGetStatusCommand}. */
@ApplicationScoped
public class OfflineGetStatusCommandResolver implements CommandResolver<OfflineGetStatusCommand> {

  @Override
  public Class<OfflineGetStatusCommand> getCommandClass() {
    return OfflineGetStatusCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, OfflineGetStatusCommand command) {
    return new OfflineGetStatusOperation(command.sessionId());
  }
}
