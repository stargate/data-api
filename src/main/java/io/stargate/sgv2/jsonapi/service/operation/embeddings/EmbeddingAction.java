package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import io.stargate.sgv2.jsonapi.service.shredding.ValueAction;
import java.util.Objects;
import java.util.function.Consumer;

public class EmbeddingAction implements ValueAction {

  private final String vectorizeText;
  private final ApiColumnDef columnDef;
  private final ApiVectorType vectorType;
  private final Consumer<float[]> vectorConsumer;

  public EmbeddingAction(
      String vectorizeText, ApiColumnDef columnDef, Consumer<float[]> vectorConsumer) {
    // TODO: AAron - you are going to need an error conumser, but the NamedValue is currently
    // tracking only ErrorCode not an exception
    this.vectorizeText = vectorizeText;

    this.vectorConsumer = vectorConsumer;

    this.columnDef = Objects.requireNonNull(columnDef, "columnDef must not be null");
    if (this.columnDef.type() instanceof ApiVectorType vt) {
      this.vectorType = vt;
    } else {
      throw new IllegalArgumentException(
          String.format(
              "columnDef.type() not a ApiVectorType, name: %s type: %s",
              columnDef.name(), columnDef.type()));
    }
    if (this.vectorType.getVectorizeDefinition() == null) {
      throw new IllegalArgumentException(
          String.format(
              "Vector column does not have vectorize Definition, name: %s type: %s",
              columnDef.name(), columnDef.type()));
    }
  }

  public VectorizeDefinition vectorizeDefinition() {
    return vectorType.getVectorizeDefinition();
  }
}
