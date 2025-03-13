package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingDeferredAction;
import io.stargate.sgv2.jsonapi.service.shredding.Deferrable;
import io.stargate.sgv2.jsonapi.service.shredding.Deferred;
import io.stargate.sgv2.jsonapi.service.shredding.DeferredAction;
import java.util.List;

public class DeferredVectorize implements Deferrable, Deferred {

  private boolean isComplete = false;

  private float[] vector = null;
  private RuntimeException exception;

  private final EmbeddingDeferredAction deferredAction;

  public DeferredVectorize(
      String vectorizeText, int dimension, VectorizeDefinition vectorizeDefinition) {

    this.deferredAction =
        new EmbeddingDeferredAction(
            vectorizeText,
            dimension,
            vectorizeDefinition,
            this::consumeEmbeddingSuccess,
            this::consumeEmbeddingFailure);
  }

  public float[] getVector() {
    checkCompleted(isComplete, "getVector()");
    return vector;
  }

  public RuntimeException exception() {
    checkCompleted(isComplete, "exception()");
    return exception;
  }

  private void consumeEmbeddingSuccess(float[] vector) {
    isComplete = maybeCompleted(isComplete, "consumeEmbeddingSuccess()");
    this.vector = vector;
  }

  private void consumeEmbeddingFailure(RuntimeException exception) {
    isComplete = maybeCompleted(isComplete, "consumeEmbeddingFailure()");
    throw new RuntimeException("Not impelemented", exception);
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
