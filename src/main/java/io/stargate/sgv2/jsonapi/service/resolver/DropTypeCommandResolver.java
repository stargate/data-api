package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropTypeCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropTypeDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.DropTypeExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.util.ApiOptionUtils;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;

/** Resolver for the {@link DropTypeCommand}. */
@ApplicationScoped
public class DropTypeCommandResolver implements CommandResolver<DropTypeCommand> {
  @Override
  public Class<DropTypeCommand> getCommandClass() {
    return DropTypeCommand.class;
  }

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, DropTypeCommand command) {

    var typeName = cqlIdentifierFromUserInput(command.name());

    var taskBuilder =
        DropTypeDBTask.builder(commandContext.schemaObject())
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
            DropTypeExceptionHandler::new, typeName));

    taskBuilder
        .withTypeName(typeName)
        .withIfExists(
            ApiOptionUtils.getOrDefault(
                command.options(), DropTypeCommand.Options::ifExists, false));

    var taskGroup = new TaskGroup<>(taskBuilder.build());

    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(DropTypeDBTask.class, commandContext));
  }
}
