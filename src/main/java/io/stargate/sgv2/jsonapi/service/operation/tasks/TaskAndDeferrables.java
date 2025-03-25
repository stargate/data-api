package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import java.util.List;
import java.util.Objects;

/**
 * Similar use to {@link TaskGroupAndDeferrables} but for when building a single task that may have
 * deferrables.
 */
public record TaskAndDeferrables<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject>(
    TaskT task, List<Deferrable> deferrables) {

  public TaskAndDeferrables(TaskT task) {
    this(task, List.of());
  }

  public TaskAndDeferrables(TaskT task, Deferrable deferrable) {
    this(task, List.of(Objects.requireNonNull(deferrable, "deferrable must not be null")));
  }
}
