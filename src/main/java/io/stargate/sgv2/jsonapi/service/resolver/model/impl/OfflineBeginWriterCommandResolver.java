package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineBeginWriterCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.OfflineBeginWriterOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link OfflineBeginWriterCommand}. */
@ApplicationScoped
public class OfflineBeginWriterCommandResolver
    implements CommandResolver<OfflineBeginWriterCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;

  @Inject
  public OfflineBeginWriterCommandResolver(Shredder shredder, ObjectMapper objectMapper) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<OfflineBeginWriterCommand> getCommandClass() {
    return OfflineBeginWriterCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, OfflineBeginWriterCommand command) {
    return new OfflineBeginWriterOperation(ctx, command, shredder, objectMapper);
  }
}
