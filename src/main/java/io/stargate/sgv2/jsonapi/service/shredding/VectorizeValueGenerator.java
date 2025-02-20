package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import java.util.function.Consumer;

public class VectorizeValueGenerator implements ValueGenerator {

  private final String vectorizeText;
  private final ApiColumnDef columnDef;
  private final Consumer<float[]> vectorConsumer;

  public VectorizeValueGenerator(
      String vectorizeText, ApiColumnDef columnDef, Consumer<float[]> vectorConsumer) {
    // TODO: AAron - you are going to need an error conumser, but the NamedValue is currently
    // tracking only ErrorCode not an exception
    this.vectorizeText = vectorizeText;
    this.columnDef = columnDef;
    this.vectorConsumer = vectorConsumer;
  }
}
