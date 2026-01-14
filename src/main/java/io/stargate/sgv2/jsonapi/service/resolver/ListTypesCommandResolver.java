package io.stargate.sgv2.jsonapi.service.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListTypesCommand;
import io.stargate.sgv2.jsonapi.service.operation.ListTypesDBTask;
import io.stargate.sgv2.jsonapi.service.operation.MetadataDBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.tables.KeyspaceDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Command resolver for the {@link
 * io.stargate.sgv2.jsonapi.api.model.command.impl.ListTypesCommand}.
 */
@ApplicationScoped
public class ListTypesCommandResolver implements CommandResolver<ListTypesCommand> {
  private final ObjectMapper objectMapper;

  @Inject
  public ListTypesCommandResolver(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  /** {@inheritDoc} */
  @Override
  public Class<ListTypesCommand> getCommandClass() {
    return ListTypesCommand.class;
  }

  /** {@inheritDoc} */
  @Override
  public Operation<KeyspaceSchemaObject> resolveKeyspaceCommand(
      CommandContext<KeyspaceSchemaObject> commandContext, ListTypesCommand command) {

    boolean explain = command.options() != null && command.options().explain();

    var taskBuilder = ListTypesDBTask.builder(commandContext.schemaObject());
    taskBuilder.withExceptionHandlerFactory(KeyspaceDriverExceptionHandler::new);
    var taskGroup = new TaskGroup<>(taskBuilder.build());

    var accumulator =
        MetadataDBTaskPage.accumulator(ListTypesDBTask.class, commandContext)
            .showSchema(explain)
            .usingCommandStatus(CommandStatus.EXISTING_TYPES);

    return new TaskOperation<>(taskGroup, accumulator);
  }
}
