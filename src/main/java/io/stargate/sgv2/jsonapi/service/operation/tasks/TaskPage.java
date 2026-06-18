package io.stargate.sgv2.jsonapi.service.operation.tasks;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Base for a page of {@link DBTask}s that have been run, and we want to get a page of results from.
 *
 * <p>Implements the {@link Supplier} interface to provide a {@link CommandResult}, which is what is
 * returned by an {@link Operation}. Subclasses should normally override the {@link
 * #buildCommandResult()} method, calling the base normally to add the errors and warnings to the
 * result.
 *
 * <p>Implements some re-usable logic for building the {@link CommandResult} .
 */
public abstract class TaskPage<TaskT extends Task<SchemaT>, SchemaT extends SchemaObject>
    implements Supplier<CommandResult> {

  protected final TaskGroup<TaskT, SchemaT> tasks;
  protected final CommandResultBuilder resultBuilder;

  /**
   * Create a new page of {@link Task} results.
   *
   * @param tasks The group of tasks that have been run.
   * @param resultBuilder The builder to use to create the {@link CommandResult}, it will have
   *     already being with any specifics for the response structure.
   */
  protected TaskPage(TaskGroup<TaskT, SchemaT> tasks, CommandResultBuilder resultBuilder) {

    this.tasks = Objects.requireNonNull(tasks, "tasks cannot be null");
    this.resultBuilder = Objects.requireNonNull(resultBuilder, "resultBuilder cannot be null");
  }

  /** Get the {@link CommandResult} that represents the page of tasks that have been run. */
  @Override
  public CommandResult get() {
    Collections.sort(tasks);
    buildCommandResult();
    return resultBuilder.build();
  }

  /**
   * Called to configure the {@link CommandResultBuilder} with the results of the tasks that have
   * been run. See the class docs.
   */
  protected void buildCommandResult() {
    addTaskErrorsToResult();
    addTaskWarningsToResult();
  }

  protected void addTaskErrorsToResult() {

    // any driver errors on the task will have been through the DriverExceptionHandler and turned
    // into ApiExceptions
    tasks.errorTasks().stream()
        .map(Task::failure)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(resultBuilder::addThrowable);
  }

  protected void addTaskWarningsToResult() {
    tasks.stream()
        .flatMap(task -> task.warningsExcludingSuppressed().stream())
        .forEach(resultBuilder::addWarning);
  }
}
