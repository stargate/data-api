package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListIndexesCommand;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import jakarta.enterprise.context.ApplicationScoped;

/** Command resolver for the {@link ListIndexesCommand}. */
@ApplicationScoped
public class ListIndexesCommandResolver implements CommandResolver<ListIndexesCommand> {

  /** {@inheritDoc} */
  @Override
  public Class<ListIndexesCommand> getCommandClass() {
    return ListIndexesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation<TableSchemaObject> resolveTableCommand(
      CommandContext<TableSchemaObject> commandContext, ListIndexesCommand command) {

    boolean explain = command.options() != null && command.options().explain();

    var taskBuilder = ListIndexesDBTask.builder(commandContext.schemaObject());
    taskBuilder.withExceptionHandlerFactory(TableDriverExceptionHandler::new);

    var tasks = new TaskGroup<ListIndexesDBTask, TableSchemaObject>(taskBuilder.build());

    var accumulator =
        MetadataAttemptPage.<TableSchemaObject>accumulator(commandContext)
            .showSchema(explain)
            .usingCommandStatus(CommandStatus.EXISTING_INDEXES);

    return new TaskOperation<>(tasks, accumulator);
  }
}
