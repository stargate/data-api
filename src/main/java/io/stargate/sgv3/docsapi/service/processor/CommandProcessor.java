package io.stargate.sgv3.docsapi.service.processor;

import io.quarkus.grpc.GrpcClient;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.resolver.CommandResolverService;
import java.util.function.Supplier;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Processes valid document {@link Command} to read, write, schema change, etc. This is a single
 * entry to run a Command without worrying how to.
 *
 * <p>Called from the API layer which deals with public JSON data formats, the command layer
 * translates from the JSON models to internal and back (shredding and de-shredding).
 *
 * <p>May provide a thread or resource boundary from calling API layer.
 */
@ApplicationScoped
public class CommandProcessor {

  private final StargateBridge stargateBridge;

  private final CommandResolverService commandResolverService;

  @Inject
  public CommandProcessor(
      @GrpcClient("bridge") StargateBridge stargateBridge,
      CommandResolverService commandResolverService) {
    this.stargateBridge = stargateBridge;
    this.commandResolverService = commandResolverService;
  }

  /**
   * Processes a single command in a given command context.
   *
   * @param commandContext {@link CommandContext}
   * @param command {@link Command}
   * @return Uni emitting the result of the command execution.
   * @param <T> Type of the command.
   */
  public <T extends Command> Uni<CommandResult> processCommand(
      CommandContext commandContext, T command) {
    // start by resolving the command, get resolver
    return commandResolverService
        .resolverForCommand(command)

        // resolver can be null, not handled in CommandResolverService for now
        .flatMap(
            resolver -> {
              // TODO this should rather throw exception that can be trasnformed to the error
              // stating that the command is not implemented
              if (null == resolver) {
                return Uni.createFrom().item(new CommandResult());
              }

              // if we have resolver, resolve operation and execute
              Operation operation = resolver.resolveCommand(commandContext, command);
              return operation
                  .execute(stargateBridge) // call supplier get to map to the command result
                  .onItem()
                  .ifNotNull()
                  .transform(Supplier::get);
            });
    // TODO if we want to catch errors here and map to CommandResult, this is the place
  }
}
