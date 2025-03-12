package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingDeferredAction;
import io.stargate.sgv2.jsonapi.service.shredding.Deferred;
import io.stargate.sgv2.jsonapi.service.shredding.DeferredAction;

public class DeferredVectorize implements Deferred {

    private final EmbeddingDeferredAction embeddingDeferredAction;

    private float[] vector = null;

    public DeferredVectorize(
        String vectorizeText,
        int dimension,
        VectorizeDefinition vectorizeDefinition) {

        this.embeddingDeferredAction = new EmbeddingDeferredAction(
            vectorizeText,
            dimension,
            vectorizeDefinition,
            this::consumeEmbeddingSuccess,
            this::consumeEmbeddingFailure);
    }

    @Override
    public DeferredAction deferredAction() {
        return embeddingDeferredAction;
    }

    public float[] getVector(){
        return vector;
    }

    private void consumeEmbeddingSuccess(float[] vector) {
        this.vector = vector;
    }

    private void consumeEmbeddingFailure(RuntimeException exception) {
        // TODO:
        throw new RuntimeException("Not impelemented", exception);
    }

}
