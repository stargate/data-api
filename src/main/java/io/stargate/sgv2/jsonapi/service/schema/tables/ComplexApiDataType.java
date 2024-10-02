package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;

/** Interface defining the api data type for complex types */
public abstract class ComplexApiDataType implements ApiDataType {
  private final String apiName;
  private final PrimitiveApiDataType keyType;
  private final PrimitiveApiDataType valueType;
  private final int vectorSize;

  public ComplexApiDataType(
      String apiName,
      PrimitiveApiDataType keyType,
      PrimitiveApiDataType valueType,
      int vectorSize) {
    this.apiName = apiName;
    this.keyType = keyType;
    this.valueType = valueType;
    this.vectorSize = vectorSize;
  }

  public PrimitiveApiDataType getKeyType() {
    return keyType;
  }

  public PrimitiveApiDataType getValueType() {
    return valueType;
  }

  public int getVectorSize() {
    return vectorSize;
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
    public VectorType(PrimitiveApiDataType valueType, int vectorSize) {
      super("vector", null, valueType, vectorSize);
    }

    @Override
    public DataType getCqlType() {
      return new ExtendedVectorType(
          ApiDataTypeDefs.from(getValueType()).get().getCqlType(), getVectorSize());
    }
  }

  /**
   * Extended vector type to support vector size This is needed because java drivers
   * DataTypes.vectorOf() method has a bug
   */
  public static class ExtendedVectorType extends DefaultVectorType {
    public ExtendedVectorType(DataType subtype, int vectorSize) {
      super(subtype, vectorSize);
    }

    @Override
    public String asCql(boolean includeFrozen, boolean pretty) {
      return "VECTOR<"
          + getElementType().asCql(includeFrozen, pretty)
          + ","
          + getDimensions()
          + ">";
    }
  }
}
