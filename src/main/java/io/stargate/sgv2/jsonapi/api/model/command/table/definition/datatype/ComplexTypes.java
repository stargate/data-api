package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.api.model.command.impl.VectorizeConfig;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.ComplexApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.PrimitiveApiDataType;

/** Interface for complex column types like collections */
public class ComplexTypes {

  /** A map type implementation */
  public static class MapType implements ColumnType {
    private final ColumnType keyType;
    private final ColumnType valueType;

    public MapType(ColumnType keyType, ColumnType valueType) {
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public ApiDataType getApiDataType() {
      return new ComplexApiDataType.MapType(
          (PrimitiveApiDataType) keyType.getApiDataType(),
          (PrimitiveApiDataType) valueType.getApiDataType());
    }

    @Override
    public String name() {
      return getApiDataType().getApiName();
    }

    public String keyType() {
      return keyType.getApiDataType().getApiName();
    }

    public String valueType() {
      return valueType.getApiDataType().getApiName();
    }
  }

  /** A list type implementation */
  public static class ListType implements ColumnType {
    private final ColumnType valueType;

    public ListType(ColumnType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ApiDataType getApiDataType() {
      return new ComplexApiDataType.ListType((PrimitiveApiDataType) valueType.getApiDataType());
    }

    @Override
    public String name() {
      return "list";
    }

    public String valueType() {
      return valueType.getApiDataType().getApiName();
    }
  }

  /** A set type implementation */
  public static class SetType implements ColumnType {
    private final ColumnType valueType;

    public SetType(ColumnType valueType) {
      this.valueType = valueType;
    }

    @Override
    public ApiDataType getApiDataType() {
      return new ComplexApiDataType.SetType((PrimitiveApiDataType) valueType.getApiDataType());
    }

    @Override
    public String name() {
      return "set";
    }

    public String valueType() {
      return valueType.getApiDataType().getApiName();
    }
  }

  /* Vector type */
  public static class VectorType implements ColumnType {
    // Float will be default type for vector
    private final ColumnType valueType;
    private final int vectorSize;
    private final VectorizeConfig vectorConfig;

    public VectorType(ColumnType valueType, int vectorSize, VectorizeConfig vectorConfig) {
      this.valueType = valueType;
      this.vectorSize = vectorSize;
      this.vectorConfig = vectorConfig;
    }

    @Override
    public ApiDataType getApiDataType() {
      return new ComplexApiDataType.VectorType(
          (PrimitiveApiDataType) valueType.getApiDataType(), vectorSize);
    }

    public VectorizeConfig getVectorConfig() {
      return vectorConfig;
    }

    @Override
    public String name() {
      return "vector";
    }

    public int getDimension() {
      return vectorSize;
    }
  }

  /**
   * Unsupported type implementation, returned in response when cql table has unsupported format
   * column
   */
  public static class UnsupportedType implements ColumnType {
    private final String cqlFormat;

    public UnsupportedType(String cqlFormat) {
      this.cqlFormat = cqlFormat;
    }

    @Override
    public ApiDataType getApiDataType() {
      throw new UnsupportedOperationException("Unsupported type");
    }

    @Override
    public String name() {
      return "UNSUPPORTED";
    }

    @Override
    public String getApiName() {
      return "UNSUPPORTED";
    }

    public String cqlFormat() {
      return cqlFormat;
    }
  }
}
