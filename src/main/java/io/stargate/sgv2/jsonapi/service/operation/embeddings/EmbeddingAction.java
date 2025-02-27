package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import static io.stargate.sgv2.jsonapi.exception.ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE;

import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import io.stargate.sgv2.jsonapi.service.shredding.ValueAction;
import io.stargate.sgv2.jsonapi.util.Recordable;
import java.util.Objects;
import java.util.function.Consumer;

public class EmbeddingAction implements ValueAction, Recordable {

  private final String vectorizeText;
  private final ApiColumnDef columnDef;
  private final ApiVectorType vectorType;
  private final Consumer<float[]> successConsumer;
  private final Consumer<RuntimeException> failureConsumer;

  private final EmbeddingActionGroupKey groupKey;

  private RuntimeException failure;

  public EmbeddingAction(
      String vectorizeText,
      ApiColumnDef columnDef,
      Consumer<float[]> successConsumer,
      Consumer<RuntimeException> failureConsumer) {
    // TODO: AAron - you are going to need an error conumser, but the NamedValue is currently
    // tracking only ErrorCode not an exception
    this.vectorizeText = vectorizeText;

    this.successConsumer = successConsumer;
    this.failureConsumer = failureConsumer;

    this.columnDef = Objects.requireNonNull(columnDef, "columnDef must not be null");
    if (this.columnDef.type() instanceof ApiVectorType vt) {
      // we use this in multiple places, get the cast done once
      this.vectorType = vt;
      if (vt.getVectorizeDefinition() == null) {
        throw new IllegalArgumentException(
            String.format(
                "Vector column does not have vectorize Definition, name: %s type: %s",
                columnDef.name(), columnDef.type()));
      }
      this.groupKey = new EmbeddingActionGroupKey(vt);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "columnDef.type() not a ApiVectorType, name: %s type: %s",
              columnDef.name(), columnDef.type()));
    }
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

    // TODO: Move to a V2 error

    if (vector.length != vectorType.getDimension()) {
      throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
          "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
          vectorType.getVectorizeDefinition().provider(), vectorType.getDimension(), vector.length);
    }
  }

  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    dataRecorder
        .append("vectorizeDefinition.provider", vectorType.getVectorizeDefinition().provider())
        .append("vectorizeDefinition.modelName", vectorType.getVectorizeDefinition().modelName())
        .append("vectorType.dimension", vectorType.getDimension());
    return dataRecorder;
  }

  public static class EmbeddingActionGroupKey {

    private final ApiVectorType vectorType;

    EmbeddingActionGroupKey(ApiVectorType vectorType) {
      this.vectorType = vectorType;
    }

    public ApiVectorType vectorType() {
      return vectorType;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      EmbeddingActionGroupKey that = (EmbeddingActionGroupKey) o;
      return Objects.equals(vectorType.getDimension(), that.vectorType.getDimension())
          && Objects.equals(
              vectorType.getVectorizeDefinition(), that.vectorType.getVectorizeDefinition());
    }
  }
}
