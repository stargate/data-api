package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.PrimitiveColumnDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.VectorColumnDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import java.util.Objects;

public class ApiVectorType extends CollectionApiDataType {

  public static final TypeFactoryFromColumnDesc<ApiVectorType, VectorColumnDesc>
      FROM_COLUMN_DESC_FACTORY = new ColumnDescFactory();
  public static final TypeFactoryFromCql<ApiVectorType, VectorType> FROM_CQL_FACTORY =
      new CqlTypeFactory();

  private final int dimension;
  private final VectorizeDefinition vectorizeDefinition;

  private ApiVectorType(int dimensions, VectorizeDefinition vectorizeDefinition) {
    super(
        ApiTypeName.VECTOR,
        ApiDataTypeDefs.FLOAT,
        new ExtendedVectorType(ApiDataTypeDefs.FLOAT.cqlType(), dimensions),
        new VectorColumnDesc(
            dimensions,
            vectorizeDefinition == null ? null : vectorizeDefinition.toVectorizeConfig()));

    this.dimension = dimensions;
    this.vectorizeDefinition = vectorizeDefinition;
  }

  public int getDimension() {
    return dimension;
  }

  public static boolean isDimensionSupported(int dimensions) {
    return dimensions > 0;
  }

  /**
   * @return Nullable VectorizeDefinition
   */
  public VectorizeDefinition getVectorizeDefinition() {
    return vectorizeDefinition;
  }

  public static ApiVectorType from(int dimension, VectorizeDefinition vectorizeDefinition) {

    // Sanity check
    if (!isDimensionSupported(dimension)) {
      throw new IllegalArgumentException(
          "dimensions is not supported, dimensions=%s".formatted(dimension));
    }
    return new ApiVectorType(dimension, vectorizeDefinition);
  }

  private static boolean isValueTypeSupported(ApiDataType valueType) {
    Objects.requireNonNull(valueType, "valueType must not be null");
    return valueType == ApiDataTypeDefs.FLOAT;
  }

  private static class ColumnDescFactory
      extends TypeFactoryFromColumnDesc<ApiVectorType, VectorColumnDesc> {

    @Override
    public ApiVectorType create(
        VectorColumnDesc columnDesc, VectorizeConfigValidator validateVectorize)
        throws UnsupportedUserType {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      // this will catch the dimensions aetc
      if (!isSupported(columnDesc, validateVectorize)) {
        throw new UnsupportedUserType(columnDesc);
      }

      var vectorDefn = VectorizeDefinition.from(columnDesc, validateVectorize);
      // TODO:  aaron mahesh - NOT SURE WHAT IS HAPPENING with VectorizeConfigValidator
      // It validates AND gets a new dimension
      var dimensions =
          columnDesc.getVectorizeConfig() == null
              ? columnDesc.getDimensions()
              : validateVectorize.validateService(
                  columnDesc.getVectorizeConfig(), columnDesc.getDimensions());
      return ApiVectorType.from(dimensions, vectorDefn);
    }

    @Override
    public boolean isSupported(
        VectorColumnDesc columnDesc, VectorizeConfigValidator validateVectorize) {
      Objects.requireNonNull(columnDesc, "columnDesc must not be null");

      return columnDesc.valueType().equals(PrimitiveColumnDesc.FLOAT)
          && columnDesc.getDimensions() >= 0;
    }
  }

  private static class CqlTypeFactory extends TypeFactoryFromCql<ApiVectorType, VectorType> {

    @Override
    public ApiVectorType create(VectorType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isSupported(cqlType)) {
        throw new UnsupportedCqlType(cqlType);
      }
      // we can ignore the element type, it is always float, checked in isSupported
      return ApiVectorType.from(cqlType.getDimensions(), vectorizeDefn);
    }

    @Override
    public boolean isSupported(VectorType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      // Must be a float
      return cqlType.getElementType() == DataTypes.FLOAT;
    }
  }
}
