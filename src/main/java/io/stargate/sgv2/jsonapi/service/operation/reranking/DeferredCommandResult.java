package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.Deferred;
import io.stargate.sgv2.jsonapi.service.shredding.DeferredAction;
import java.util.List;

public class DeferredCommandResult implements Deferrable, Deferred {

  private boolean isComplete = false;

  private CommandResult commandResult;
  private RuntimeException exception;

  private final DeferredCommandResultAction deferredAction;

  public DeferredCommandResult() {
    this.deferredAction =
        new DeferredCommandResultAction(this::consumeReadSuccess, this::consumeReadFailure);
  }

  public CommandResult commandResult() {
    checkCompleted(isComplete, "commandResult()");
    return commandResult;
  }

  public RuntimeException exception() {
    checkCompleted(isComplete, "exception()");
    return exception;
  }

  private void consumeReadSuccess(CommandResult commandResult) {
    isComplete = maybeCompleted(isComplete, "consumeReadSuccess()");
    this.commandResult = commandResult;
  }

  private void consumeReadFailure(RuntimeException exception) {
    isComplete = maybeCompleted(isComplete, "consumeReadFailure()");
    this.exception = exception;
  }

  @Override
  public List<? extends Deferred> deferred() {
    return List.of(this);
  }

  @Override
  public DeferredAction deferredAction() {
    return deferredAction;
  }
}
