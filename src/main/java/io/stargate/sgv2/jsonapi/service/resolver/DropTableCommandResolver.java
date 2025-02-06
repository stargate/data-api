package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropTableCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/** Resolver for the {@link DropTableCommand}. */
@ApplicationScoped
public class DropTableCommandResolver implements CommandResolver<DropTableCommand> {
  @Override
  public Class<DropTableCommand> getCommandClass() {
    return DropTableCommand.class;
  }

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, DropTableCommand command) {

    var tableName = cqlIdentifierFromUserInput(command.name());

    var taskBuilder = DropTableDBTask.builder(commandContext.schemaObject())
        .withSchemaRetryPolicy(
            new SchemaDBTask.SchemaRetryPolicy(
                commandContext.getConfig(OperationsConfig.class).databaseConfig().ddlRetries(),
                Duration.ofMillis(
                    commandContext
                        .getConfig(OperationsConfig.class)
                        .databaseConfig()
                        .ddlRetryDelayMillis())));

    taskBuilder.withExceptionHandlerFactory(DefaultDriverExceptionHandler.Factory.withIdentifier(
        DropTableExceptionHandler::new, tableName));

    taskBuilder
        .withTableName(tableName)
        .withIfExists(
            ApiOptionUtils.getOrDefault(command.options(),
                DropTableCommand.Options::ifExists, false));

    var taskGroup = new TaskGroup<>(taskBuilder.build());

    return new TaskOperation<>(
        taskGroup,
        SchemaDBTaskPage.accumulator(DropTableDBTask.class,commandContext));
  }
}
