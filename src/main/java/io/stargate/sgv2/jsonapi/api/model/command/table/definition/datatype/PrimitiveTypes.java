package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.service.schema.tables.ApiDataType;
import io.stargate.sgv2.jsonapi.service.schema.tables.PrimitiveApiDataType;
import java.util.HashMap;
import java.util.Map;

/** Interface for primitive column types similar to what is defined in cassandra java driver. */
public enum PrimitiveTypes implements ColumnType {

  // TODO: add a private ctor to stop this class from being instantiated or make abstract

  ASCII(PrimitiveApiDataType.ASCII),
  BIGINT(PrimitiveApiDataType.BIGINT),
  BINARY(PrimitiveApiDataType.BINARY),
  BOOLEAN(PrimitiveApiDataType.BOOLEAN),
  DATE(PrimitiveApiDataType.DATE),
  DECIMAL(PrimitiveApiDataType.DECIMAL),
  DOUBLE(PrimitiveApiDataType.DOUBLE),
  DURATION(PrimitiveApiDataType.DURATION),
  FLOAT(PrimitiveApiDataType.FLOAT),
  INET(PrimitiveApiDataType.INET),
  INT(PrimitiveApiDataType.INT),
  SMALLINT(PrimitiveApiDataType.SMALLINT),
  TEXT(PrimitiveApiDataType.TEXT),
  TIME(PrimitiveApiDataType.TIME),
  TIMESTAMP(PrimitiveApiDataType.TIMESTAMP),
  TINYINT(PrimitiveApiDataType.TINYINT),
  UUID(PrimitiveApiDataType.UUID),
  VARINT(PrimitiveApiDataType.VARINT);

  @Override
  public ApiDataType getApiDataType() {
    return getApiDataType;
  }

  PrimitiveTypes(ApiDataType getApiDataType) {
    this.getApiDataType = getApiDataType;
  }

  private ApiDataType getApiDataType;

  private static Map<String, ColumnType> columnTypeMap = new HashMap<>();

  static {
    for (PrimitiveTypes type : PrimitiveTypes.values()) {
      columnTypeMap.put(type.getApiDataType().getApiName(), type);
    }
  }

  public static ColumnType fromString(String type) {
    return columnTypeMap.get(type);
  }
}
