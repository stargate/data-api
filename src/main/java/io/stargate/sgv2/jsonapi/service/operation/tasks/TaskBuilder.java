package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DefaultDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extensible base for builders to create {@link Task} objects. */
public abstract class TaskBuilder<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject> {
  private static final Logger LOGGER = LoggerFactory.getLogger(TaskBuilder.class);

  // first value is zero, but we increment before we use it
  private int taskPosition = -1;

  // No default to make sure that we remember that set it to something specific if needed.
  private DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory = null;

  protected final SchemaT schemaObject;

  /** */
  protected TaskBuilder(SchemaT schemaObject) {
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject must not be null");
  }

  protected int nextPosition() {
    return taskPosition += 1;
  }

  protected DefaultDriverExceptionHandler.Factory<SchemaT> getExceptionHandlerFactory() {
    if (exceptionHandlerFactory == null) {
      throw new IllegalStateException("exceptionHandlerFactory must be set");
    }
    return exceptionHandlerFactory;
  }

  @SuppressWarnings("unchecked")
  public <T extends TaskBuilder<TaskT, SchemaT>> T withExceptionHandlerFactory(
      DefaultDriverExceptionHandler.Factory<SchemaT> exceptionHandlerFactory) {
    this.exceptionHandlerFactory = exceptionHandlerFactory;
    return (T) this;
  }

  /**
   * Task builder when the task only has positionID and schemaObject.
   *
   * @param <TaskT>
   * @param <SchemaT>
   */
  public static class BasicTaskBuilder<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
      extends TaskBuilder<TaskT, SchemaT> {

    protected final BasicTaskConstructor<TaskT, SchemaT> taskFactory;

    public BasicTaskBuilder(
        SchemaT schemaObject, BasicTaskConstructor<TaskT, SchemaT> taskFactory) {
      super(schemaObject);
      this.taskFactory = Objects.requireNonNull(taskFactory, "taskFactory must not be null");
    }

    public TaskT build() {
      return taskFactory.create(nextPosition(), schemaObject, getExceptionHandlerFactory());
    }

    @FunctionalInterface
    public interface BasicTaskConstructor<
        TaskT extends Task<SchemaT>, SchemaT extends SchemaObject> {
      TaskT create(
          int position,
          SchemaT schemaObject,
          DefaultDriverExceptionHandler.Factory<TableSchemaObject> exceptionHandlerFactory);
    }
  }
}
