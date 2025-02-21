package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import java.util.function.Supplier;

/** */
public class CompositeTaskAccumulator<
        TaskT extends CompositeTask<SchemaT>, SchemaT extends TableBasedSchemaObject>
    extends TaskAccumulator<TaskT, SchemaT> {

  protected CompositeTaskAccumulator() {}

  @Override
  public Supplier<CommandResult> getResults() {
    // we have a group of tasks, those tasks as wrapping operations that in turn ran a group of
    // tasks.
    // the last task in the group should be the one that gets the results to return to the user
    // e.g. we did some embedding, then did inserts, we want the results of the inserts to send to
    // the user

    // TODO: what to I do to check for error etc ?
    return () -> tasks.getLast().getInnerOperationResult();
  }
}
