package io.stargate.sgv2.jsonapi.service.processor;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.DeprecatedCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.TraceMessage;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APIExceptionCommandErrorBuilder;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger logger = LoggerFactory.getLogger(CommandProcessor.class);

  private final DataVectorizerService dataVectorizerService;

  private final CommandResolverService commandResolverService;

  @Inject
  public CommandProcessor(
      CommandResolverService commandResolverService, DataVectorizerService dataVectorizerService) {
    this.commandResolverService = commandResolverService;
    this.dataVectorizerService = dataVectorizerService;
  }

  /**
   * Processes a single command in a given command context.
   *
   * @param commandContext {@link CommandContext}
   * @param command {@link Command}
   * @return Uni emitting the result of the command execution.
   * @param <CommandT> Type of the command.
   * @param <SchemaT> Type of the schema object command operates on.
   */
  public <CommandT extends Command, SchemaT extends SchemaObject> Uni<CommandResult> processCommand(
      CommandContext<SchemaT> commandContext, CommandT command) {

    var debugMode = commandContext.config().get(DebugModeConfig.class).enabled();
    var errorObjectV2 = commandContext.config().get(OperationsConfig.class).extendError();

    commandContext
        .requestTracing()
        .maybeTrace(
            () ->
                new TraceMessage(
                    "Starting to process '%s' command for schema object %s"
                        .formatted(
                            command.commandName().getApiName(),
                            PrettyPrintable.print(commandContext.schemaObject().name())),
                    commandContext.schemaObject()));

    // First bit of pre-processing: possible expansion of "$hybrid" alias
    HybridFieldExpander.expandHybridField(commandContext, command);

    // vectorize the data
    return dataVectorizerService
        .vectorize(commandContext, command)
        .onItem()
        .transformToUni(
            vectorizedCommand -> {
              // start by resolving the command, get resolver
              return commandResolverService
                  .resolverForCommand(vectorizedCommand)

                  // resolver can be null, not handled in CommandResolverService for now
                  .flatMap(
                      resolver -> {
                        // if we have resolver, resolve operation
                        Operation<SchemaT> operation =
                            resolver.resolveCommand(commandContext, vectorizedCommand);
                        return Uni.createFrom().item(operation);
                      });
            })

        //  execute the operation
        .flatMap(operation -> operation.execute(commandContext))

        // handle failures here
        .onFailure()
        .recoverWithItem(
            t ->
                switch (t) {
                  case APIException apiException -> {
                    // new error object V2
                    var errorBuilder =
                        new APIExceptionCommandErrorBuilder(debugMode, errorObjectV2);

                    // yet more mucking about with suppliers everywhere :(
                    yield (Supplier<CommandResult>)
                        () ->
                            CommandResult.statusOnlyBuilder(
                                    errorObjectV2, debugMode, commandContext.requestTracing())
                                .addCommandResultError(
                                    errorBuilder.buildLegacyCommandResultError(apiException))
                                .build();
                  }
                  case JsonApiException jsonApiException ->
                      // old error objects, old comment below
                      // Note: JsonApiException means that JSON API itself handled the situation
                      // (created, or wrapped the exception) -- should not be logged (have already
                      // been logged if necessary)
                      jsonApiException;
                  default -> {
                    // Old error handling below, to be replaced eventually (aaron aug 28 2024)
                    // But other exception types are unexpected, so log for now
                    logger.warn(
                        "Command '{}' failed with exception",
                        command.getClass().getSimpleName(),
                        t);
                    yield new ThrowableCommandResultSupplier(t);
                  }
                })

        // if we have a non-null item
        // call supplier get to map to the command result
        .onItem()
        .ifNotNull()
        .transform(Supplier::get)
        // add possible warning for using a deprecated command
        .map(
            commandResult -> {
              if (command instanceof DeprecatedCommand deprecatedCommand) {
                // for the warnings we always want V2 errors and do not want / need debug ?
                var errorV2 =
                    new APIExceptionCommandErrorBuilder(false, true)
                        .buildCommandErrorV2(deprecatedCommand.getDeprecationWarning());
                commandResult.addWarning(errorV2);
              }
              return commandResult;
            });
  }
}
