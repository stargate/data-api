package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineEndWriterCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.OfflineEndWriterOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link OfflineEndWriterCommand}. */
@ApplicationScoped
public class OfflineEndWriterCommandResolver implements CommandResolver<OfflineEndWriterCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;

  @Inject
  public OfflineEndWriterCommandResolver(Shredder shredder, ObjectMapper objectMapper) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<OfflineEndWriterCommand> getCommandClass() {
    return OfflineEndWriterCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, OfflineEndWriterCommand command) {
    return new OfflineEndWriterOperation(ctx, command, shredder, objectMapper);
  }
}
