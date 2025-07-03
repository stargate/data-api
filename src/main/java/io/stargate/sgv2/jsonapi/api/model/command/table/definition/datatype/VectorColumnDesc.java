package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.Objects;

/** Column type for {@link ApiVectorType} */
public class VectorColumnDesc extends ComplexColumnDesc {
  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  // Float will be default type for vector
  private final ColumnDesc valueType;
  private final Integer dimension;
  private final VectorizeConfig vectorizeConfig;

  public VectorColumnDesc(Integer dimension, VectorizeConfig vectorizeConfig) {
    this(
        dimension, vectorizeConfig, ApiSupportDesc.withoutCqlDefinition(ApiVectorType.API_SUPPORT));
  }

  public VectorColumnDesc(
      Integer dimension, VectorizeConfig vectorizeConfig, ApiSupportDesc apiSupportDesc) {
    super(ApiTypeName.VECTOR, apiSupportDesc);

    this.valueType = PrimitiveColumnDesc.FLOAT;
    this.dimension = dimension;
    this.vectorizeConfig = vectorizeConfig;
  }

  public VectorizeConfig getVectorizeConfig() {
    return vectorizeConfig;
  }

  public Integer getDimension() {
    return dimension;
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
    return Objects.equals(dimension, vectDesc.dimension)
        && Objects.equals(valueType, vectDesc.valueType)
        && Objects.equals(vectorizeConfig, vectDesc.vectorizeConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(valueType, dimension, vectorizeConfig);
  }

  /**
   * Factory to create a {@link VectorColumnDesc} from JSON representing the type
   *
   * <p>...
   */
  public static class FromJsonFactory {
    FromJsonFactory() {}

    public VectorColumnDesc create(String dimensionString, VectorizeConfig vectorConfig) {
      // if the string is null/empty/blank, we think the user doesn't provide the dimension
      // we will check later if the null dimension is allowed
      // when we create the ApiVectorType
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

      // aaron- not calling ApiVectorType isSupported because the value type is locked to float
      return new VectorColumnDesc(dimension, vectorConfig);
    }
  }
}
