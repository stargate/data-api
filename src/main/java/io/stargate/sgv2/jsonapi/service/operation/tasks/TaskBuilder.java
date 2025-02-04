package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Exensible base for builders to create {@link Task} objects.
 */
public abstract class TaskBuilder<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskBuilder.class);

  // first value is zero, but we increment before we use it
  private int taskPosition = -1;

  protected final SchemaT schemaObject;

  /**
   */
  protected TaskBuilder(SchemaT schemaObject) {
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject must not be null");
  }

  protected int nextPosition() {
    return taskPosition += 1;
  }

  public static class BasicTaskBuilder <TaskT extends Task<SchemaT>, SchemaT extends SchemaObject> extends TaskBuilder<TaskT, SchemaT> {

    protected final BiFunction<Integer, SchemaT, TaskT> taskFactory;

    public BasicTaskBuilder(SchemaT schemaObject, BiFunction<Integer, SchemaT, TaskT> taskFactory) {
      super(schemaObject);
      this.taskFactory = Objects.requireNonNull(taskFactory, "taskFactory must not be null");
    }

    public TaskT build() {
      return taskFactory.apply(nextPosition(), schemaObject);
    }

  }
}
