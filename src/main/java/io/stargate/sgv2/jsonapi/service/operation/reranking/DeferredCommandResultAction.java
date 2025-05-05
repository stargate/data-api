package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.service.shredding.DeferredAction;
import java.util.function.Consumer;

public class DeferredCommandResultAction implements DeferredAction {

  Consumer<CommandResult> successConsumer;
  Consumer<RuntimeException> failureConsumer;

  public DeferredCommandResultAction(
      Consumer<CommandResult> successConsumer, Consumer<RuntimeException> failureConsumer) {
    this.successConsumer = successConsumer;
    this.failureConsumer = failureConsumer;
  }

  public void setEmptyMultiDocumentResponse() {
    successConsumer.accept(
        CommandResult.multiDocumentBuilder(true, false, RequestTracing.NO_OP).build());
  }

  public void onSuccess(CommandResult commandResult) {
    successConsumer.accept(commandResult);
  }

  public void onFailure(RuntimeException exception) {
    failureConsumer.accept(exception);
  }
}
