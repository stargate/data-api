package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.VectorType;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;
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
        null);
    // passes null for the columnDesc, the vector type is not cached and we only need the column
    // desc
    // when returning metadata so may not need it for general read and write (i.e. we will create
    // the column def
    // for the table even if we dont read / write the vector
    // create the column dec on demand
    this.dimension = dimensions;
    this.vectorizeDefinition = vectorizeDefinition;
  }

  @Override
  public ColumnDesc columnDesc() {
    return new VectorColumnDesc(
        dimension, vectorizeDefinition == null ? null : vectorizeDefinition.toVectorizeConfig());
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

  public static ApiVectorType from(int dimension) {
    return from(dimension, null);
  }

  public static ApiVectorType from(int dimension, VectorizeDefinition vectorizeDefinition) {

    // Sanity check
    if (!isDimensionSupported(dimension)) {
      throw new IllegalArgumentException(
          "dimensions is not supported, dimensions=%s".formatted(dimension));
    }
    return new ApiVectorType(dimension, vectorizeDefinition);
  }

  /**
   * This method is only used for Dec hotfix. Currently, we don't support multiple vectorize service
   * config. 1. If one doesn't have vectorizeDefinition (use user's own vector) and the other one
   * have (use embedding service), it's ok 2. If both have vectorizeDefinition, only support if they
   * are the same and have the same dimension
   *
   * @param v the other vector type to compare
   * @return true if the two vector types are supported together
   */
  public boolean isTwoVectorColumnSupported(ApiVectorType v) {
    if (this.vectorizeDefinition == null || v.getVectorizeDefinition() == null) {
      return true;
    }
    return this.dimension == v.getDimension()
        && Objects.equals(this.vectorizeDefinition, v.getVectorizeDefinition());
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
