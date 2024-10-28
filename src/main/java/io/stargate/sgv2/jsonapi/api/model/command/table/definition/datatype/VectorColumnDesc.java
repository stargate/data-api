package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.Map;
import java.util.Objects;

/** Column type for {@link ApiVectorType} */
public class VectorColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  // Float will be default type for vector
  private final ColumnDesc valueType;
  private final int dimensions;
  private final VectorizeConfig vectorizeConfig;

  public VectorColumnDesc(int dimensions, VectorizeConfig vectorizeConfig) {
    super(ApiTypeName.VECTOR, ApiSupportDesc.fullSupport(""));

    this.valueType = PrimitiveColumnDesc.FLOAT;
    this.dimensions = dimensions;
    this.vectorizeConfig = vectorizeConfig;
  }

  public VectorizeConfig getVectorizeConfig() {
    return vectorizeConfig;
  }

  public int getDimensions() {
    return dimensions;
  }

  public ColumnDesc valueType() {
    return valueType;
  }

  // Needed for testing
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.VectorColumnDesc that =
        (io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.VectorColumnDesc) o;
    return dimensions == that.dimensions
        && Objects.equals(valueType, that.valueType)
        && Objects.equals(vectorizeConfig, that.vectorizeConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueType, dimensions, vectorizeConfig);
  }

  public static class FromJsonFactory {
    FromJsonFactory() {}

    public VectorColumnDesc create(String dimensionString, VectorizeConfig vectorConfig) {

      Integer dimension = null;
      try {
        dimension = Integer.parseInt(dimensionString);
      } catch (NumberFormatException e) {
        // handle below
      }
      if (dimension == null || !ApiVectorType.isDimensionSupported(dimension)) {
        throw SchemaException.Code.UNSUPPORTED_VECTOR_DIMENSION.get(
            Map.of("unsupportedValue", String.valueOf(dimensionString)));
      }

      // TODO: AARON / MAHESH Where is the vector config valdiated?

      // aaron- not calling ApiVectorType isSupported because the value type is locked to float
      return new VectorColumnDesc(dimension, vectorConfig);
    }
  }
}
