package io.stargate.sgv2.jsonapi.service.processor;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APIExceptionCommandErrorBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizerService;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
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

  private final QueryExecutor queryExecutor;

  private final DataVectorizerService dataVectorizerService;

  private final CommandResolverService commandResolverService;

  @Inject
  public CommandProcessor(
      QueryExecutor queryExecutor,
      CommandResolverService commandResolverService,
      DataVectorizerService dataVectorizerService) {
    this.queryExecutor = queryExecutor;
    this.commandResolverService = commandResolverService;
    this.dataVectorizerService = dataVectorizerService;
  }

  /**
   * Processes a single command in a given command context.
   *
   * @param commandContext {@link CommandContext}
   * @param command {@link Command}
   * @return Uni emitting the result of the command execution.
   * @param <T> Type of the command.
   * @param <U> Type of the schema object command operates on.
   */
  public <T extends Command, U extends SchemaObject> Uni<CommandResult> processCommand(
      DataApiRequestInfo dataApiRequestInfo, CommandContext<U> commandContext, T command) {
    // vectorize the data
    return dataVectorizerService
        .vectorize(dataApiRequestInfo, commandContext, command)
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
                        Operation operation =
                            resolver.resolveCommand(commandContext, vectorizedCommand);
                        return Uni.createFrom().item(operation);
                      });
            })

        //  execute the operation
        .flatMap(operation -> operation.execute(dataApiRequestInfo, queryExecutor))

        // handle failures here
        .onFailure()
        .recoverWithItem(
            t ->
                switch (t) {
                  case APIException apiException -> {
                    // new error object V2
                    var errorBuilder =
                        new APIExceptionCommandErrorBuilder(
                            commandContext.getConfig(DebugModeConfig.class).enabled(),
                            commandContext.getConfig(OperationsConfig.class).extendError());

                    // yet more mucking about with suppliers everywhere :(
                    yield (Supplier<CommandResult>)
                        () -> new CommandResult(List.of(errorBuilder.apply(apiException)));
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
        .transform(Supplier::get);
  }
}
