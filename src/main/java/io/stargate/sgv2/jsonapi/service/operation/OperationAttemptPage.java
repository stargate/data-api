package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Supplier;

abstract class OperationAttemptPage<
        SchemaT extends SchemaObject, AttemptT extends OperationAttempt<AttemptT, SchemaT>>
    implements Supplier<CommandResult> {

  protected final OperationAttemptContainer<SchemaT, AttemptT> attempts;
  protected final CommandResultBuilder resultBuilder;

  protected OperationAttemptPage(
      OperationAttemptContainer<SchemaT, AttemptT> attempts, CommandResultBuilder resultBuilder) {

    this.attempts = attempts;
    this.resultBuilder = resultBuilder;
  }

  @Override
  public CommandResult get() {
    Collections.sort(attempts);
    buildCommandResult();
    return resultBuilder.build();
  }

  protected void buildCommandResult() {
    addAttemptErrorsToResult();
    addAttemptWarningsToResult();
  }

  protected void addAttemptErrorsToResult() {
    attempts.errorAttempts().stream()
        .map(OperationAttempt::failure)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(resultBuilder::addThrowable);
  }

  protected void addAttemptWarningsToResult() {
    attempts.stream()
        .flatMap(attempt -> attempt.warnings().stream())
        .forEach(resultBuilder::addWarning);
  }
}
