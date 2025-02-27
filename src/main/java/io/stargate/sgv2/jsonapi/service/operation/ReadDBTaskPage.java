package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.DBTaskPage;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskGroup;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskOperation;
import java.util.*;

/**
 * A page of results from a {@link ReadDBTask }, use {@link #builder()} to get a builder to pass to
 * {@link TaskOperation}.
 */
public class ReadDBTaskPage<SchemaT extends TableBasedSchemaObject>
    extends DBTaskPage<ReadDBTask<SchemaT>, SchemaT> {

  private final CqlPagingState pagingState;
  private final boolean includeSortVector;
  private final float[] sortVector;

  private ReadDBTaskPage(
      TaskGroup<ReadDBTask<SchemaT>, SchemaT> tasks,
      CommandResultBuilder resultBuilder,
      CqlPagingState pagingState,
      boolean includeSortVector,
      float[] sortVector) {
    super(tasks, resultBuilder);
    this.pagingState = pagingState;
    this.includeSortVector = includeSortVector;
    this.sortVector = sortVector;
  }

  public static <SchemaT extends TableSchemaObject> Accumulator<SchemaT> accumulator(
      CommandContext<SchemaT> commandContext) {
    return TaskAccumulator.configureForContext(new Accumulator<>(), commandContext);
  }

  @Override
  protected void buildCommandResult() {

    super.buildCommandResult();

    if (includeSortVector && sortVector != null) {
      resultBuilder.addStatus(CommandStatus.SORT_VECTOR, sortVector);
    }
    pagingState.getPagingStateString().ifPresent(resultBuilder::nextPageState);
    maybeAddSortedRowCount();
    maybeAddSchema(CommandStatus.PROJECTION_SCHEMA);

    tasks.completedTasks().stream()
        .flatMap(task -> task.documents().stream())
        .forEach(resultBuilder::addDocument);
  }

  protected void maybeAddSortedRowCount() {

    var rowCounts =
        tasks.completedTasks().stream()
            .map(ReadDBTask::sortedRowCount)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .toList();

    if (rowCounts.isEmpty()) {
      return;
    }

    // not the best place for this check, it should be done earlier, but if we did more than one in
    // memory sort they
    // are not merged together. Currently, we are not fanning reads out to multiple attempt so this
    // is not a problem.
    if (rowCounts.size() > 1) {
      throw new IllegalStateException(
          "ReadDBTaskPage.maybeAddSortedRowCount() - Multiple sorted row counts, counts="
              + rowCounts);
    }

    var sortedRowCount = rowCounts.getFirst();
    resultBuilder.addStatus(CommandStatus.SORTED_ROW_COUNT, sortedRowCount);
  }

  /**
   * Builder for {@link ReadDBTaskPage} - it takes state into the processing of a task group so it
   * can be used after processing.
   */
  public static class Accumulator<SchemaT extends TableBasedSchemaObject>
      extends TaskAccumulator<ReadDBTask<SchemaT>, SchemaT> {

    private boolean singleResponse = false;
    private boolean includeSortVector;
    private float[] sortVector;

    protected Accumulator() {}

    public Accumulator<SchemaT> singleResponse(boolean singleResponse) {
      this.singleResponse = singleResponse;
      return this;
    }

    private Accumulator<SchemaT> includeSortVector(boolean includeSortVector) {
      this.includeSortVector = includeSortVector;
      return this;
    }

    private Accumulator<SchemaT> sortVector(float[] sortVector) {
      this.sortVector = sortVector;
      return this;
    }

    public <CmdT extends VectorSortable> Accumulator<SchemaT> mayReturnVector(CmdT command) {
      var includeVector = command.includeSortVector().orElse(false);
      if (includeVector) {
        var requestedVector =
            command.vectorSortExpression().map(SortExpression::vector).orElse(null);
        if (requestedVector != null) {
          this.includeSortVector = true;
          this.sortVector = requestedVector;
        }
      }
      return this;
    }

    @Override
    public ReadDBTaskPage<SchemaT> getResults() {

      var nonEmptyPageStateAttempts =
          tasks.completedTasks().stream()
              .filter(task -> !task.resultPagingState().isEmpty())
              .toList();

      var pagingState =
          switch (nonEmptyPageStateAttempts.size()) {
            case 0 -> CqlPagingState.EMPTY;
            case 1 -> nonEmptyPageStateAttempts.getFirst().resultPagingState();
            default ->
                throw new IllegalStateException(
                    "ReadDBTaskPage.Builder.build() - Multiple ReadAttempts with non-empty paging state, attempts="
                        + String.join(
                            ", ",
                            nonEmptyPageStateAttempts.stream().map(Object::toString).toList()));
          };

      var resultBuilder =
          singleResponse
              ? CommandResult.singleDocumentBuilder(useErrorObjectV2, debugMode, requestTracing)
              : CommandResult.multiDocumentBuilder(useErrorObjectV2, debugMode, requestTracing);

      return new ReadDBTaskPage<>(tasks, resultBuilder, pagingState, includeSortVector, sortVector);
    }
  }
}
