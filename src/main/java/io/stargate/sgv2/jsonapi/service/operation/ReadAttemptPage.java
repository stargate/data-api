package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import java.util.*;
import java.util.function.Supplier;

public class ReadAttemptPage<SchemaT extends TableBasedSchemaObject>
    implements Supplier<CommandResult> {

  private final OperationAttemptContainer<SchemaT, ReadAttempt<SchemaT>> attempts;
  private final CqlPagingState pagingState;
  private final boolean includeSortVector;
  private final float[] sortVector;

  private final CommandResultBuilder resultBuilder;

  private ReadAttemptPage(
      OperationAttemptContainer<SchemaT, ReadAttempt<SchemaT>> attempts,
      CqlPagingState pagingState,
      boolean includeSortVector,
      float[] sortVector,
      CommandResultBuilder resultBuilder) {

    this.attempts = attempts;
    this.pagingState = pagingState;
    this.includeSortVector = includeSortVector;
    this.sortVector = sortVector;
    this.resultBuilder = resultBuilder;
  }

  public static <SchemaT extends TableBasedSchemaObject> Builder<SchemaT> builder() {
    return new Builder<>();
  }

  @Override
  public CommandResult get() {

    if (includeSortVector && sortVector != null) {
      resultBuilder.addStatus(CommandStatus.SORT_VECTOR, sortVector);
    }
    pagingState.getPagingStateString().ifPresent(resultBuilder::nextPageState);

    attempts.completedAttempts().stream()
        .flatMap(attempt -> attempt.documents().stream())
        .forEach(resultBuilder::addDocument);

    attempts.errorAttempts().stream()
        .map(ReadAttempt::failure)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(resultBuilder::addThrowable);

    return resultBuilder.build();
  }

  public static class Builder<SchemaT extends TableBasedSchemaObject>
      extends OperationAttemptAccumulator<SchemaT, ReadAttempt<SchemaT>> {

    private boolean singleResponse = false;
    private boolean useErrorObjectV2 = false;
    private boolean debugMode = false;
    private boolean includeSortVector;
    private float[] sortVector;

    Builder() {}

    public Builder<SchemaT> singleResponse(boolean singleResponse) {
      this.singleResponse = singleResponse;
      return this;
    }

    public Builder<SchemaT> useErrorObjectV2(boolean useErrorObjectV2) {
      this.useErrorObjectV2 = useErrorObjectV2;
      return this;
    }

    public Builder<SchemaT> debugMode(boolean debugMode) {
      this.debugMode = debugMode;
      return this;
    }

    public Builder<SchemaT> includeSortVector(boolean includeSortVector) {
      this.includeSortVector = includeSortVector;
      return this;
    }

    public Builder<SchemaT> sortVector(float[] sortVector) {
      this.sortVector = sortVector;
      return this;
    }

    public ReadAttemptPage<SchemaT> build() {

      attempts.checkAllAttemptsTerminal();

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

      var resultBuilder = new CommandResultBuilder(singleResponse, useErrorObjectV2, debugMode);

      return new ReadAttemptPage<>(
          attempts, pagingState, includeSortVector, sortVector, resultBuilder);
    }
  }
}
