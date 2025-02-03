package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * base for a page of {@link DBTask}s that have been run, subclasses are used to configure
 * the {@link CommandResult} as needed for each command type.
 *
 * <p>Implements the {@link Supplier} interface to provide a {@link CommandResult}, which is what is
 * returned by an {@link Operation}. Subclasses should normally override the {@link
 * #buildCommandResult()} method, calling the base normally to add the errors and warnings to the
 * result.
 *
 * <p>Implements some re-usable logic for building the {@link CommandResult} from the {@link
 * OperationAttempt}s.
 */
abstract class DBTaskPage<
    TaskT extends DBTask<SchemaT>,
    SchemaT extends SchemaObject>
    implements Supplier<CommandResult> {

  protected final TaskGroup<TaskT, SchemaT> tasks;
  protected final CommandResultBuilder resultBuilder;

  protected DBTaskPage(
      TaskGroup<TaskT, SchemaT> tasks, CommandResultBuilder resultBuilder) {

    this.tasks = Objects.requireNonNull(tasks, "attempts cannot be null");
    this.resultBuilder = Objects.requireNonNull(resultBuilder, "resultBuilder cannot be null");
  }

  /** Get the {@link CommandResult} that represents the page of attempts that have been run. */
  @Override
  public CommandResult get() {
    Collections.sort(tasks);
    buildCommandResult();
    return resultBuilder.build();
  }

  /**
   * Called to configure the {@link CommandResultBuilder} with the results of the attempts that have
   * been run. See the class docs.
   */
  protected void buildCommandResult() {
    addAttemptErrorsToResult();
    addAttemptWarningsToResult();
  }

  protected void addAttemptErrorsToResult() {

    // any driver errors on the attempt will have been through the DriverExceptionHandler and turned
    // into ApiExceptions
    tasks.errorTasks().stream()
        .map(Task::failure)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(resultBuilder::addThrowable);
  }

  protected void addAttemptWarningsToResult() {
    tasks.stream()
        .flatMap(attempt -> attempt.warningsExcludingSuppressed().stream())
        .forEach(resultBuilder::addWarning);
  }

  /**
   * Adds the schema for the first attempt that returns a schema description.
   *
   * <p>Uses the first, not the first successful, because we may fail to do an insert but will still
   * have the _id or PK to report.
   */
  protected void maybeAddSchema(CommandStatus statusKey) {
    if (tasks.isEmpty()) {
      return;
    }

    tasks.stream()
        .map(DBTask::schemaDescription)
        .filter(Optional::isPresent)
        .findFirst()
        .ifPresent(object -> resultBuilder.addStatus(statusKey, object));
  }
}
