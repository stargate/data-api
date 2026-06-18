package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;

/**
 * Test task for testing the core functionality of the {@link BaseTask} - not testing any of the
 * subclasses.
 */
public class BaseTaskTestTask
    extends BaseTask<TableSchemaObject, BaseTask.UniSupplier<String>, String> {

  // The result the task will return , passed through the BaseTask process.
  // for a DBTask this would be a result set
  public final String TASK_RESULT = "task-result-" + System.nanoTime();

  BaseTaskTestTask(int position, TableSchemaObject schemaObject, TaskRetryPolicy retryPolicy) {
    super(position, schemaObject, retryPolicy);
    // Note: nothing here about the exception mappers, that idea is in the DBTask, this is testing
    // the very base class
  }

  @Override
  protected UniSupplier<String> buildResultSupplier(
      CommandContext<TableSchemaObject> commandContext) {
    return () -> Uni.createFrom().item(TASK_RESULT);
  }

  @Override
  protected Throwable maybeHandleException(
      UniSupplier<String> resultSupplier, Throwable throwable) {
    throw new RuntimeException("Not implemented");
  }
}
