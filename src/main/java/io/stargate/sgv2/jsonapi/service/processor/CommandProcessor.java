package io.stargate.sgv2.jsonapi.service.processor;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandErrorFactory;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.DeprecatedCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.TraceMessage;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.ExceptionFlags;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
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
   * @param command The command to be processed.
   * @param <CommandT> Type of the command.
   * @param <SchemaT> Type of the schema object the command operates on.
   * @return A {@link Uni} emitting the {@link CommandResult} of the command execution.
   */
  public <CommandT extends Command, SchemaT extends SchemaObject> Uni<CommandResult> processCommand(
      CommandContext<SchemaT> commandContext, CommandT command) {

    // Initial tracing before the reactive pipeline starts
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

    return Uni.createFrom()
        .item(command)
        .onItem()

        // Step 1: Expand any hybrid fields in the command (synchronous) and record the command
        // features
        .invoke(
            cmd -> {
              HybridFieldExpander.expandHybridField(commandContext, cmd);
              cmd.addCommandFeatures(commandContext.commandFeatures());
            })

        // Step 2: Vectorize relevant parts of the command (asynchronous)
        .flatMap(cmd -> dataVectorizerService.vectorize(commandContext, cmd))

        // Step 3: Resolve the vectorized command to a runnable Operation (asynchronous)
        .flatMap(cmd -> resolveCommandToOperation(commandContext, cmd))

        // Step 4: Execute the operation (asynchronous)
        .flatMap(operation -> operation.execute(commandContext))

        // Step 5: Handle any failures from the preceding steps
        .onFailure()
        .recoverWithItem(throwable -> handleProcessingFailure(commandContext, command, throwable))

        // Step 6: Transform the successful or recovered item (Supplier<CommandResult>) into
        // CommandResult
        .onItem()
        .ifNotNull()
        .transform(Supplier::get)

        // Step 7: Perform any final post-processing on the CommandResult (e.g., add warnings)
        .map(commandResult -> postProcessCommandResult(command, commandResult));
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

    var resultBuilder = CommandResult.statusOnlyBuilder(commandContext.requestTracing());

    return switch (throwable) {
      case APIException apiException -> {
        // Check if session should be evicted before building the error response
        maybeEvictSession(commandContext, apiException);

        yield () -> resultBuilder.addThrowable(apiException).build();
      }
      case JsonApiException jsonApiException -> {
        // old error objects, old comment below
        // Note: JsonApiException means that JSON API itself handled the situation
        // (created, or wrapped the exception) -- should not be logged (have already
        // been logged if necessary)
        maybeEvictSession(commandContext, jsonApiException.getCause());
        yield () -> resultBuilder.addThrowable(jsonApiException).build();
      }
      default -> {
        // Old error handling below, to be replaced eventually (aaron aug 28 2024)
        // But other exception types are unexpected, so log for now
        logger.warn(
            "Command '{}' failed with exception",
            originalCommand.getClass().getSimpleName(),
            throwable);

        maybeEvictSession(commandContext, throwable);
        yield () -> resultBuilder.addThrowable(throwable).build();
      }
    };
  }

  /**
   * Evicts the CQL session when the {@link APIException} indicates the current session is
   * unreliable (for example, when all cluster nodes have restarted).
   *
   * @param commandContext The command context.
   * @param apiException The API exception that may contain exception flags.
   * @param <SchemaT> The schema object type.
   */
  private <SchemaT extends SchemaObject> void maybeEvictSession(
      CommandContext<SchemaT> commandContext, APIException apiException) {

    // exceptionFlags is guaranteed to be non-null by ErrorInstance and APIException constructors
    if (apiException.exceptionFlags.contains(ExceptionFlags.UNRELIABLE_DB_SESSION)) {
      commandContext.cqlSessionCache().evictSession(commandContext.requestContext());
    }
  }

  /**
   * Evicts the CQL session when the throwable is {@link AllNodesFailedException}, which indicates
   * the current session is unreliable (for example, when all cluster nodes have restarted).
   */
  private <SchemaT extends SchemaObject> void maybeEvictSession(
      CommandContext<SchemaT> commandContext, Throwable throwable) {

    if (throwable instanceof AllNodesFailedException) {
      commandContext.cqlSessionCache().evictSession(commandContext.requestContext());
    }
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
      // we already have the result, not a result builder, so need to create the warning error here
      // not on hot path, can create the factory each time
      commandResult.addWarning(
          new CommandErrorFactory().create(deprecatedCommand.getDeprecationWarning()));
    }
    return commandResult;
  }
}
