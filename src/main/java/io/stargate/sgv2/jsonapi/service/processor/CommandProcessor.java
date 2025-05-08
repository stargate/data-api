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
 * Processes a {@link Command} by taking it through a series of transformations: expansion,
 * vectorization, resolution to an {@link Operation}, and finally execution. It also handles error
 * recovery and result post-processing.
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
   * Processes a single command through the full pipeline.
   *
   * @param commandContext The context for the command execution.
   * @param initialCommand The command to be processed.
   * @param <CommandT> Type of the command.
   * @param <SchemaT> Type of the schema object the command operates on.
   * @return A {@link Uni} emitting the {@link CommandResult} of the command execution.
   */
  public <CommandT extends Command, SchemaT extends SchemaObject> Uni<CommandResult> processCommand(
      CommandContext<SchemaT> commandContext, CommandT initialCommand) {

    // Initial tracing before the reactive pipeline starts
    commandContext
        .requestTracing()
        .maybeTrace(
            () ->
                new TraceMessage(
                    "Starting to process '%s' command for schema object %s"
                        .formatted(
                            initialCommand.commandName().getApiName(),
                            PrettyPrintable.print(commandContext.schemaObject().name())),
                    commandContext.schemaObject()));

    return Uni.createFrom()
        .item(initialCommand)
        .onItem()

        // Step 1: Expand any hybrid fields in the command (synchronous, in-place modification)
        .invoke(
            commandToExpand -> {
              HybridFieldExpander.expandHybridField(commandContext, commandToExpand);
            })

        // Step 2: Vectorize relevant parts of the command (asynchronous)
        .flatMap(
            expandedCommand -> dataVectorizerService.vectorize(commandContext, expandedCommand))

        // Step 3: Resolve the vectorized command to a runnable Operation (asynchronous)
        .flatMap(vectorizedCommand -> resolveCommandToOperation(commandContext, vectorizedCommand))

        // Step 4: Execute the operation (asynchronous)
        .flatMap(operation -> operation.execute(commandContext))

        // Step 5: Handle any failures from the preceding steps
        .onFailure()
        .recoverWithItem(
            throwable -> handleProcessingFailure(commandContext, initialCommand, throwable))

        // Step 6: Transform the successful or recovered item (Supplier<CommandResult>) into
        // CommandResult
        .onItem()
        .ifNotNull()
        .transform(Supplier::get)

        // Step 7: Perform any final post-processing on the CommandResult (e.g., add warnings)
        .map(commandResult -> postProcessCommandResult(initialCommand, commandResult));
  }

  /**
   * Resolves a {@link Command} to its corresponding {@link Operation}.
   *
   * @param commandContext The command context.
   * @param commandToResolve The command to resolve.
   * @param <SchemaT> Type of the schema object.
   * @return A {@link Uni} emitting the resolved {@link Operation}.
   */
  private <SchemaT extends SchemaObject> Uni<Operation<SchemaT>> resolveCommandToOperation(
      CommandContext<SchemaT> commandContext, Command commandToResolve) {
    return commandResolverService
        // Find resolver for command, it handles the case where resolver is null
        .resolverForCommand(commandToResolve)
        .flatMap(
            resolver -> {
              // Now the resolver is found, resolve the command to an operation.
              // This resolution step itself is synchronous.
              Operation<SchemaT> operation =
                  resolver.resolveCommand(commandContext, commandToResolve);
              return Uni.createFrom().item(operation);
            });
  }

  /**
   * Handles failures that occur during the command processing pipeline. It attempts to convert
   * known exceptions (APIException, JsonApiException) into a {@link CommandResult} supplier, and
   * logs other unexpected exceptions.
   *
   * @param commandContext The command context.
   * @param originalCommand The initial command that was being processed.
   * @param throwable The failure.
   * @return A {@link Supplier} of {@link CommandResult} representing the error.
   */
  private <SchemaT extends SchemaObject> Supplier<CommandResult> handleProcessingFailure(
      CommandContext<SchemaT> commandContext, Command originalCommand, Throwable throwable) {
    var debugMode = commandContext.config().get(DebugModeConfig.class).enabled();
    var errorObjectV2 = commandContext.config().get(OperationsConfig.class).extendError();

    return switch (throwable) {
      case APIException apiException -> {
        // new error object V2
        var errorBuilder = new APIExceptionCommandErrorBuilder(debugMode, errorObjectV2);
        yield () ->
            CommandResult.statusOnlyBuilder(
                    errorObjectV2, debugMode, commandContext.requestTracing())
                .addCommandResultError(errorBuilder.buildLegacyCommandResultError(apiException))
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
            originalCommand.getClass().getSimpleName(),
            throwable);
        yield new ThrowableCommandResultSupplier(throwable);
      }
    };
  }

  /**
   * Performs post-processing on the {@link CommandResult}, such as adding warnings for deprecated
   * commands.
   *
   * @param originalCommand The initial command that was processed.
   * @param commandResult The result of the command execution.
   * @return The potentially modified {@link CommandResult}.
   */
  private CommandResult postProcessCommandResult(
      Command originalCommand, CommandResult commandResult) {
    if (originalCommand instanceof DeprecatedCommand deprecatedCommand) {
      // (aaron) for the warnings we always want V2 errors and do not want / need debug ?
      var errorV2 =
          new APIExceptionCommandErrorBuilder(false, true)
              .buildCommandErrorV2(deprecatedCommand.getDeprecationWarning());
      commandResult.addWarning(errorV2);
    }
    return commandResult;
  }
}
