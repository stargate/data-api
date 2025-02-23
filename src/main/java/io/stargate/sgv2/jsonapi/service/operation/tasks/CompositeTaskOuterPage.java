package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

public class CompositeTaskOuterPage<SchemaT extends SchemaObject>
    implements Supplier<CommandResult> {

  private final TaskGroup<CompositeTask<?, SchemaT>, SchemaT> tasks;

  private CompositeTaskOuterPage(TaskGroup<CompositeTask<?, SchemaT>, SchemaT> tasks) {
    this.tasks = tasks;
  }

  public static <SchemaT extends SchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {

    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  public CommandResult get() {
    return tasks.getLast().innerPage().get();
  }

  public static class Accumulator<SchemaT extends SchemaObject>
      extends TaskAccumulator<CompositeTask<?, SchemaT>, SchemaT> {

    protected Accumulator() {}

    @Override
    public Supplier<CommandResult> getResults() {
      return new CompositeTaskOuterPage<>(tasks);
    }
  }
}
