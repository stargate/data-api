package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.VectorSortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.*;

/**
 * A page of results from a read command, use {@link #builder()} to get a builder to pass to {@link
 * GenericOperation}.
 */
public class ReadAttemptPage<SchemaT extends TableSchemaObject>
    extends OperationAttemptPage<SchemaT, ReadAttempt<SchemaT>> {

  private final CqlPagingState pagingState;
  private final boolean includeSortVector;
  private final float[] sortVector;

  private ReadAttemptPage(
      OperationAttemptContainer<SchemaT, ReadAttempt<SchemaT>> attempts,
      CommandResultBuilder resultBuilder,
      CqlPagingState pagingState,
      boolean includeSortVector,
      float[] sortVector) {
    super(attempts, resultBuilder);
    this.pagingState = pagingState;
    this.includeSortVector = includeSortVector;
    this.sortVector = sortVector;
  }

  public static <SchemaT extends TableSchemaObject> Builder<SchemaT> builder() {
    return new Builder<>();
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

    attempts.completedAttempts().stream()
        .flatMap(attempt -> attempt.documents().stream())
        .forEach(resultBuilder::addDocument);
  }

  protected void maybeAddSortedRowCount() {

    var rowCounts =
        attempts.completedAttempts().stream()
            .map(ReadAttempt::sortedRowCount)
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
          "ReadAttemptPage.maybeAddSortedRowCount() - Multiple sorted row counts, counts="
              + rowCounts);
    }

    var sortedRowCount = rowCounts.getFirst();
    resultBuilder.addStatus(CommandStatus.SORTED_ROW_COUNT, sortedRowCount);
  }

  public static class Builder<SchemaT extends TableSchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, ReadAttempt<SchemaT>> {

    private boolean singleResponse = false;
    private boolean includeSortVector;
    private float[] sortVector;

    Builder() {}

    public Builder<SchemaT> singleResponse(boolean singleResponse) {
      this.singleResponse = singleResponse;
      return this;
    }

    private Builder<SchemaT> includeSortVector(boolean includeSortVector) {
      this.includeSortVector = includeSortVector;
      return this;
    }

    private Builder<SchemaT> sortVector(float[] sortVector) {
      this.sortVector = sortVector;
      return this;
    }

    public <CmdT extends VectorSortable> Builder<SchemaT> mayReturnVector(CmdT command) {
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
    public ReadAttemptPage<SchemaT> getOperationPage() {

      var nonEmptyPageStateAttempts =
          attempts.completedAttempts().stream()
              .filter(attempts -> !attempts.resultPagingState().isEmpty())
              .toList();

      var pagingState =
          switch (nonEmptyPageStateAttempts.size()) {
            case 0 -> CqlPagingState.EMPTY;
            case 1 -> nonEmptyPageStateAttempts.getFirst().resultPagingState();
            default ->
                throw new IllegalStateException(
                    "ReadAttemptPage.Builder.build() - Multiple ReadAttempts with non-empty paging state, attempts="
                        + String.join(
                            ", ",
                            nonEmptyPageStateAttempts.stream().map(Object::toString).toList()));
          };

      var resultBuilder =
          singleResponse
              ? CommandResult.singleDocumentBuilder(debugMode)
              : CommandResult.multiDocumentBuilder(debugMode);

      return new ReadAttemptPage<>(
          attempts, resultBuilder, pagingState, includeSortVector, sortVector);
    }
  }
}
