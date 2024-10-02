package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

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
  }

  /* Vector type */
  public static class VectorType implements ColumnType {
    // Float will be default type for vector
    private final ColumnType valueType;
    private final int vectorSize;

    public VectorType(ColumnType valueType, int vectorSize) {
      this.valueType = valueType;
      this.vectorSize = vectorSize;
    }

    @Override
    public ApiDataType getApiDataType() {
      return new ComplexApiDataType.VectorType(
          (PrimitiveApiDataType) valueType.getApiDataType(), vectorSize);
    }
  }
}
