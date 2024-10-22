package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedVectorType;

/** Interface defining the api data type for complex types */
public abstract class ComplexApiDataType implements ApiDataType {
  private final String apiName;
  private final PrimitiveApiDataType keyType;
  private final PrimitiveApiDataType valueType;
  private final int dimension;

  public ComplexApiDataType(
      String apiName, PrimitiveApiDataType keyType, PrimitiveApiDataType valueType, int dimension) {
    this.apiName = apiName;
    this.keyType = keyType;
    this.valueType = valueType;
    this.dimension = dimension;
  }

  public PrimitiveApiDataType getKeyType() {
    return keyType;
  }

  public PrimitiveApiDataType getValueType() {
    return valueType;
  }

  public int getDimension() {
    return dimension;
  }

  public abstract DataType getCqlType();

  @Override
  public String getApiName() {
    return apiName;
  }

  public static class MapType extends ComplexApiDataType {
    public MapType(PrimitiveApiDataType keyType, PrimitiveApiDataType valueType) {
      super("map", keyType, valueType, -1);
    }

    @Override
    public DataType getCqlType() {
      return DataTypes.mapOf(
          ApiDataTypeDefs.from(getKeyType()).get().getCqlType(),
          ApiDataTypeDefs.from(getValueType()).get().getCqlType());
    }
  }

  public static class ListType extends ComplexApiDataType {
    public ListType(PrimitiveApiDataType valueType) {
      super("list", null, valueType, -1);
    }

    @Override
    public DataType getCqlType() {
      return DataTypes.listOf(ApiDataTypeDefs.from(getValueType()).get().getCqlType());
    }
  }

  public static class SetType extends ComplexApiDataType {
    public SetType(PrimitiveApiDataType valueType) {
      super("set", null, valueType, -1);
    }

    @Override
    public DataType getCqlType() {
      return DataTypes.setOf(ApiDataTypeDefs.from(getValueType()).get().getCqlType());
    }
  }

  public static class VectorType extends ComplexApiDataType {
    public VectorType(PrimitiveApiDataType valueType, int dimension) {
      super("vector", null, valueType, dimension);
    }

    @Override
    public DataType getCqlType() {
      return new ExtendedVectorType(
          ApiDataTypeDefs.from(getValueType()).get().getCqlType(), getDimension());
    }
  }
}
