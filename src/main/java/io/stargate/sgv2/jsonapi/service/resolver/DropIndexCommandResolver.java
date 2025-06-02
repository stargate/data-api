package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropIndexDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropIndexExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/** Resolver for the {@link DropIndexCommand}. */
@ApplicationScoped
public class DropIndexCommandResolver implements CommandResolver<DropIndexCommand> {

  private static final boolean IF_EXISTS_DEFAULT = false;

  @Override
  public Class<DropIndexCommand> getCommandClass() {
    return DropIndexCommand.class;
  }

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, DropIndexCommand command) {

    var indexName = cqlIdentifierFromUserInput(command.name());
    // Check if the index exists, we check if columns exist before trying to drop them so do for
    // indexes as well

    var taskBuilder =
        DropIndexDBTask.builder(commandContext.schemaObject())
            .withSchemaRetryPolicy(
                new SchemaDBTask.SchemaRetryPolicy(
                    commandContext
                        .config()
                        .get(OperationsConfig.class)
                        .databaseConfig()
                        .ddlRetries(),
                    Duration.ofMillis(
                        commandContext
                            .config()
                            .get(OperationsConfig.class)
                            .databaseConfig()
                            .ddlRetryDelayMillis())));

    taskBuilder.withExceptionHandlerFactory(
        DefaultDriverExceptionHandler.Factory.withIdentifier(
            DropIndexExceptionHandler::new, indexName));

    taskBuilder
        .withIndexName(indexName)
        .withIfExists(
            ApiOptionUtils.getOrDefault(
                command.options(), DropIndexCommand.Options::ifExists, IF_EXISTS_DEFAULT));

    var taskGroup = new TaskGroup<>(taskBuilder.build());

    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(DropIndexDBTask.class, commandContext));
  }
}
