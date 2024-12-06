package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Column type for {@link ApiVectorType} */
public class VectorColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  // Float will be default type for vector
  private final ColumnDesc valueType;
  private final Integer dimensions;
  private final VectorizeConfig vectorizeConfig;

  public VectorColumnDesc(Integer dimensions, VectorizeConfig vectorizeConfig) {
    super(ApiTypeName.VECTOR, ApiSupportDesc.fullSupport(""));

    this.valueType = PrimitiveColumnDesc.FLOAT;
    this.dimensions = dimensions;
    this.vectorizeConfig = vectorizeConfig;
  }

  public VectorizeConfig getVectorizeConfig() {
    return vectorizeConfig;
  }

  public Optional<Integer> getDimensions() {
    return Optional.ofNullable(dimensions);
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
    var vectDesc = (VectorColumnDesc) o;
    return Objects.equals(dimensions, vectDesc.dimensions)
        && Objects.equals(valueType, vectDesc.valueType)
        && Objects.equals(vectorizeConfig, vectDesc.vectorizeConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueType, dimensions, vectorizeConfig);
  }

  public static class FromJsonFactory {
    FromJsonFactory() {}

    public VectorColumnDesc create(String dimensionString, VectorizeConfig vectorConfig) {
      // if the string is null/empty/blank, we think the user doesn't provide the dimension
      // we will check later if the null dimension is allowed
      // same logic as the collection
      if (dimensionString == null || dimensionString.isBlank()) {
        return new VectorColumnDesc(null, vectorConfig);
      }

      // dimensionString cannot be non-empty string or the value is negative
      Integer dimension = null;
      try {
        dimension = Integer.parseInt(dimensionString);
      } catch (NumberFormatException e) {
        // handle below
      }
      if (dimension == null || !ApiVectorType.isDimensionSupported(dimension)) {
        throw SchemaException.Code.UNSUPPORTED_VECTOR_DIMENSION.get(
            Map.of("unsupportedValue", dimensionString));
      }
      // aaron- not calling ApiVectorType isSupported because the value type is locked to float
      return new VectorColumnDesc(dimension, vectorConfig);
    }
  }
}
