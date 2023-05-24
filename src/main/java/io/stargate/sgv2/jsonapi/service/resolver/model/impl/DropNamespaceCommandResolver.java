package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropNamespaceCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DropNamespaceOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;

/** Command resolver for {@link DropNamespaceCommand}. */
@ApplicationScoped
public class DropNamespaceCommandResolver implements CommandResolver<DropNamespaceCommand> {

  /** {@inheritDoc} */
  @Override
  public Class<DropNamespaceCommand> getCommandClass() {
    return DropNamespaceCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation resolveCommand(CommandContext ctx, DropNamespaceCommand command) {
    return new DropNamespaceOperation(command.name());
  }
}
