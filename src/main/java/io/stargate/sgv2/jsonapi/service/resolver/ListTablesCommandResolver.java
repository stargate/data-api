package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListTablesCommand;
import io.stargate.sgv2.jsonapi.service.operation.*;
import io.stargate.sgv2.jsonapi.service.operation.tables.KeyspaceDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Command resolver for the {@link ListTablesCommand}. */
@ApplicationScoped
public class ListTablesCommandResolver implements CommandResolver<ListTablesCommand> {
  private final ObjectMapper objectMapper;

  @Inject
  public ListTablesCommandResolver(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /** {@inheritDoc} */
  @Override
  public Class<ListTablesCommand> getCommandClass() {
    return ListTablesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, ListTablesCommand command) {

    boolean explain = command.options() != null && command.options().explain();

    var taskBuilder = ListTablesDBTask.builder(commandContext.schemaObject());
    taskBuilder.withExceptionHandlerFactory(KeyspaceDriverExceptionHandler::new);
    var taskGroup = new TaskGroup<>(taskBuilder.build());

    var accumulator =
        MetadataDBTaskPage.accumulator(ListTablesDBTask.class, commandContext)
            .showSchema(explain)
            .usingCommandStatus(CommandStatus.EXISTING_TABLES);

    return new TaskOperation<>(taskGroup, accumulator);
  }
}
