package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTypeCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTypeDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTypeExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;

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
            .withTypeName(udtName);

    var renamingFields = command.rename() != null ? command.rename().fields() : null;
    var addingFields = command.add() != null ? command.add().fields() : null;

    var allRenamesForHandler =
        renamingFields != null ? renamingFields.keySet().stream().toList() : List.<String>of();
    var allAddFieldsForHandler =
        addingFields != null ? addingFields.keySet().stream().toList() : List.<String>of();

    taskBuilder =
        taskBuilder.withExceptionHandlerFactory(
            ((keyspaceSchemaObject, simpleStatement) ->
                new AlterTypeExceptionHandler(
                    keyspaceSchemaObject,
                    simpleStatement,
                    udtName,
                    allRenamesForHandler,
                    allAddFieldsForHandler)));

    // cannot use taskBuilder in lambda
    // build tasks for renaming fields
    if (renamingFields != null) {
      for (var entry : renamingFields.entrySet()) {
        taskGroup.add(taskBuilder.buildForRenameField(entry.getKey(), entry.getValue()));
      }
    }

    // build tasks for adding fields
    if (command.add() != null
        && command.add().fields() != null
        && !command.add().fields().isEmpty()) {
      var apiUdtType =
          ApiUdtType.FROM_TYPE_DESC_FACTORY.create(
              command.name(), command.add(), validateVectorize);

      for (var fieldDef : apiUdtType.allFields().values()) {
        taskGroup.add(taskBuilder.buildForAddField(fieldDef));
      }
    }

    // sanity check
    if (taskGroup.isEmpty()) {
      throw SchemaException.Code.MISSING_ALTER_TYPE_OPERATIONS.get();
    }

    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(AlterTypeDBTask.class, commandContext));
  }

  @Override
  public Class<AlterTypeCommand> getCommandClass() {
    return AlterTypeCommand.class;
  }
}
