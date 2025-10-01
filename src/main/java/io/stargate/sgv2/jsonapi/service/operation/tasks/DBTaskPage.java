package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.Optional;

public abstract class DBTaskPage<TaskT extends DBTask<SchemaT>, SchemaT extends SchemaObject>
    extends TaskPage<TaskT, SchemaT> {

  protected DBTaskPage(TaskGroup<TaskT, SchemaT> taskGroup, CommandResultBuilder resultBuilder) {
    super(taskGroup, resultBuilder);
  }

  /**
   * Adds the schema for the first task that returns a schema description.
   *
   * <p>Uses the first, not the first successful, because we may fail to do an insert but will still
   * have the _id or PK to report.
   */
  protected void maybeAddSchema(CommandStatus statusKey) {
    if (taskGroup.tasks().isEmpty()) {
      return;
    }

    taskGroup.tasks().stream()
        .map(DBTask::schemaDescription)
        .filter(Optional::isPresent)
        .findFirst()
        .ifPresent(object -> resultBuilder.addStatus(statusKey, object));
  }
}
