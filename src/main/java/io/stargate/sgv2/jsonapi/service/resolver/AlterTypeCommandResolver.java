package io.stargate.sgv2.jsonapi.service.resolver;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTypeCommand;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTypeDBTask;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTypeDBTaskBuilder;
import io.stargate.sgv2.jsonapi.service.operation.tables.AlterTypeExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiUdtDef;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Resolver for the {@link AlterTypeCommand}. Note, this resolver is used to handle both renaming
 * and adding fields in a UDT. It builds a {@link TaskGroup} containing multiple {@link
 * AlterTypeDBTask} instances, they are executed in parallel, so the command may have partial
 * success and return errors for failed tasks.
 */
@ApplicationScoped
public class AlterTypeCommandResolver implements CommandResolver<AlterTypeCommand> {

  @Inject VectorizeConfigValidator validateVectorize;

  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, AlterTypeCommand command) {

    //    if ((command.rename() == null
    //            || command.rename().fields() == null
    //            || command.rename().fields().isEmpty())
    //        && (command.add() == null
    //            || command.add().fields() == null
    //            || command.add().fields().isEmpty())) {
    //      throw new IllegalArgumentException(" options must be provided for AlterTypeCommand");
    //    }

    var typeIdentifier = cqlIdentifierFromUserInput(command.name());

    // Note, renaming-field and adding-field are separate AlterTypeDBTask.
    // So we are building a TaskGroup with multiple tasks.
    List<AlterTypeDBTask> tasks = new ArrayList<>();

    // build tasks for renaming fields
    Map<String, String> renamingFields =
        command.rename() != null ? command.rename().fields() : null;
    if (renamingFields != null) {
      for (Map.Entry<String, String> renameEntry : renamingFields.entrySet()) {
        var taskBuilder =
            initAlterTypeDBTaskBuilder(commandContext, typeIdentifier)
                .withTypeName(typeIdentifier)
                .withRenamingField(
                    Map.entry(
                        cqlIdentifierFromUserInput(renameEntry.getKey()),
                        cqlIdentifierFromUserInput(renameEntry.getValue())));
        tasks.add(taskBuilder.buildForRenamingField());
      }
    }

    // build tasks for adding fields
    ColumnsDescContainer addingFields = command.add() != null ? command.add().fields() : null;
    if (addingFields != null && !addingFields.isEmpty()) {
      // create the ApiUdtDef from the AlterType add definition
      var apiUdtDef =
          ApiUdtDef.FROM_TYPE_DESC_FACTORY.create(command.name(), command.add(), validateVectorize);

      for (ApiColumnDef addingField : apiUdtDef.allFields().values()) {

        var taskBuilder =
            initAlterTypeDBTaskBuilder(commandContext, typeIdentifier)
                .withTypeName(typeIdentifier)
                .withAddingField(addingField);
        tasks.add(taskBuilder.buildForAddingField());
      }
    }

    var taskGroup = new TaskGroup<>(tasks);
    return new TaskOperation<>(
        taskGroup, SchemaDBTaskPage.accumulator(AlterTypeDBTask.class, commandContext));
  }

  /**
   * Initialize the {@link AlterTypeDBTaskBuilder} with the provided command context. RetryPolicy
   * will be set with initialization.
   */
  private AlterTypeDBTaskBuilder initAlterTypeDBTaskBuilder(
      CommandContext<KeyspaceSchemaObject> commandContext, CqlIdentifier typeName) {
    return AlterTypeDBTask.builder(commandContext.schemaObject())
        .withSchemaRetryPolicy(
            new SchemaDBTask.SchemaRetryPolicy(
                commandContext.config().get(OperationsConfig.class).databaseConfig().ddlRetries(),
                Duration.ofMillis(
                    commandContext
                        .config()
                        .get(OperationsConfig.class)
                        .databaseConfig()
                        .ddlRetryDelayMillis())))
        .withExceptionHandlerFactory(
            DefaultDriverExceptionHandler.Factory.withIdentifier(
                AlterTypeExceptionHandler::new, typeName));
  }

  @Override
  public Class<AlterTypeCommand> getCommandClass() {
    return AlterTypeCommand.class;
  }
}
