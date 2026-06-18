package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import io.stargate.sgv2.jsonapi.service.shredding.DeferredAction;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class EmbeddingDeferredAction implements DeferredAction, Recordable {

  private final String vectorizeText;
  private final int dimension;
  private final VectorizeDefinition vectorizeDefinition;
  private final Consumer<float[]> successConsumer;
  private final Consumer<RuntimeException> failureConsumer;

  private final EmbeddingActionGroupKey groupKey;

  private RuntimeException failure;

  public EmbeddingDeferredAction(
      String vectorizeText,
      ApiVectorType vectorType,
      Consumer<float[]> successConsumer,
      Consumer<RuntimeException> failureConsumer) {
    this(
        vectorizeText,
        Objects.requireNonNull(vectorType, "vectorType must not be null").getDimension(),
        Objects.requireNonNull(vectorType, "vectorType must not be null").getVectorizeDefinition(),
        successConsumer,
        failureConsumer);
  }

  public EmbeddingDeferredAction(
      String vectorizeText,
      Integer dimension,
      VectorizeDefinition vectorizeDefinition,
      Consumer<float[]> successConsumer,
      Consumer<RuntimeException> failureConsumer) {
    // TODO: AAron - you are going to need an error conumser, but the NamedValue is currently
    // tracking only ErrorCode not an exception
    this.vectorizeText = vectorizeText;

    this.successConsumer = successConsumer;
    this.failureConsumer = failureConsumer;

    this.dimension = dimension;
    this.vectorizeDefinition =
        Objects.requireNonNull(vectorizeDefinition, "vectorizeDefinition must not be null");
    this.groupKey = new EmbeddingActionGroupKey(dimension, vectorizeDefinition);
  }

  public EmbeddingActionGroupKey groupKey() {
    return groupKey;
  }

  public String startEmbedding() {
    return vectorizeText;
  }

  /**
   * Called to set the vector for this action, that represents the text.
   *
   * <p>Validates the vector against what we expect and throws an exception if not valid.
   *
   * @param vector vector to replace the text with
   */
  public void onSuccess(float[] vector) {
    try {
      validateVector(vector);
      successConsumer.accept(vector);
    } catch (RuntimeException e) {
      setFailure(e);
      throw e;
    }
  }

  private void setFailure(RuntimeException failure) {
    if (this.failure != null) {
      this.failure = failure;
      failureConsumer.accept(failure);
    }
  }

  /**
   * Run all rules for validating the vector is what we expected.
   *
   * @param vector created from the text this acton supplied.
   */
  private void validateVector(float[] vector) {

    if (vector.length != dimension) {
      throw EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.get(
          Map.of(
              "errorMessage",
              "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'"
                  .formatted(vectorizeDefinition.provider(), dimension, vector.length)));
    }
  }

  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    dataRecorder
        .append("vectorizeDefinition.provider", vectorizeDefinition.provider())
        .append("vectorizeDefinition.modelName", vectorizeDefinition.modelName())
        .append("vectorizeDefinition.parameters", vectorizeDefinition.parameters())
        .append("vectorizeDefinition.authentication", vectorizeDefinition.authentication())
        .append("dimension", dimension);
    return dataRecorder;
  }

  public static class EmbeddingActionGroupKey implements Recordable {

    private final int dimension;
    private final VectorizeDefinition vectorizeDefinition;

    EmbeddingActionGroupKey(int dimension, VectorizeDefinition vectorizeDefinition) {
      this.dimension = dimension;
      this.vectorizeDefinition = vectorizeDefinition;
    }

    public int dimension() {
      return dimension;
    }

    public VectorizeDefinition vectorizeDefinition() {
      return vectorizeDefinition;
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("vectorizeDefinition", vectorizeDefinition)
          .append("dimension", dimension);
    }

    @Override
    public String toString() {
      return PrettyPrintable.pprint(this, false);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dimension, vectorizeDefinition);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EmbeddingActionGroupKey that = (EmbeddingActionGroupKey) o;
      return Objects.equals(dimension, that.dimension)
          && Objects.equals(vectorizeDefinition, that.vectorizeDefinition);
    }
  }
}
