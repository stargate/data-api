package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTypeCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTypeDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTypeExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;

/**
 * Resolver for the {@link AlterTypeCommand}.
 *
 * <p>Note: this resolver is used to handle both renaming and adding fields in a UDT. It builds a
 * {@link TaskGroup} containing multiple {@link AlterTypeDBTask} instances, they are executed
 * sequentially as they are all making schema changes, so the command may have partial success and
 * return errors for failed tasks.
 */
@ApplicationScoped
public class AlterTypeCommandResolver implements CommandResolver<AlterTypeCommand> {

  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, AlterTypeCommand command) {

    var udtName = cqlIdentifierFromUserInput(command.name());
    var taskGroup = new TaskGroup<AlterTypeDBTask, KeyspaceSchemaObject>(true);
    var taskBuilder =
        AlterTypeDBTask.builder(commandContext.schemaObject())
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
                            .ddlRetryDelayMillis())))
            .withExceptionHandlerFactory(
                DefaultDriverExceptionHandler.Factory.withIdentifier(
                    AlterTypeExceptionHandler::new, udtName))
            .withTypeName(udtName);

    // build tasks for renaming fields
    var renamingFields = command.rename() != null ? command.rename().fields() : null;
    if (renamingFields != null) {

      renamingFields.forEach(
          (key, value) -> taskGroup.add(taskBuilder.buildForRenameField(key, value)));
    }

    // build tasks for adding fields
    if (command.add() != null && !command.add().fields().isEmpty()) {
      var apiUdtType =
          ApiUdtType.FROM_TYPE_DESC_FACTORY.create(
              command.name(), command.add(), validateVectorize);

      apiUdtType
          .allFields()
          .values()
          .forEach(fieldDef -> taskGroup.add(taskBuilder.buildForAddField(fieldDef)));
    }

    // sanity check
    if (taskGroup.isEmpty()) {
      // TODO: XXXX: AARON: what eerror to throw here?
      throw new IllegalArgumentException(
          "AlterTypeCommand must have at least one field to add or rename");
    }

    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(AlterTypeDBTask.class, commandContext));
  }

  @Override
  public Class<AlterTypeCommand> getCommandClass() {
    return AlterTypeCommand.class;
  }
}
