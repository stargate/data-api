package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.HashMap;
import java.util.Map;

/** Interface for primitive column types similar to what is defined in cassandra java driver. */
public enum PrimitiveColumnTypes implements ColumnType {
  ASCII(ApiDataTypeDefs.ASCII),
  BIGINT(ApiDataTypeDefs.BIGINT),
  BINARY(ApiDataTypeDefs.BINARY),
  BOOLEAN(ApiDataTypeDefs.BOOLEAN),
  DATE(ApiDataTypeDefs.DATE),
  DECIMAL(ApiDataTypeDefs.DECIMAL),
  DOUBLE(ApiDataTypeDefs.DOUBLE),
  DURATION(ApiDataTypeDefs.DURATION),
  FLOAT(ApiDataTypeDefs.FLOAT),
  INET(ApiDataTypeDefs.INET),
  INT(ApiDataTypeDefs.INT),
  SMALLINT(ApiDataTypeDefs.SMALLINT),
  TEXT(ApiDataTypeDefs.TEXT),
  TIME(ApiDataTypeDefs.TIME),
  TIMESTAMP(ApiDataTypeDefs.TIMESTAMP),
  TINYINT(ApiDataTypeDefs.TINYINT),
  UUID(ApiDataTypeDefs.UUID),
  VARINT(ApiDataTypeDefs.VARINT);

  private static Map<ApiDataType, PrimitiveColumnTypes> BY_API_TYPE = new HashMap<>();
  private static Map<String, PrimitiveColumnTypes> BY_API_TYPE_NAME = new HashMap<>();

  static {
    for (PrimitiveColumnTypes type : PrimitiveColumnTypes.values()) {
      BY_API_TYPE.put(type.apiDataType, type);
      BY_API_TYPE_NAME.put(type.apiDataType.getName().getApiName(), type);
    }
  }

  private final ApiDataType apiDataType;

  PrimitiveColumnTypes(ApiDataType apiDataType) {
    this.apiDataType = apiDataType;
  }

  @Override
  public ApiDataTypeName getApiDataTypeName() {
    return apiDataType.getName();
  }

  public static ColumnType fromApiDataType(ApiDataType apiDataType) {
    if (!apiDataType.isPrimitive()) {
      throw new IllegalArgumentException("Not a primitive type: " + apiDataType);
    }
    // sanity check that we have all the API types
    if (!BY_API_TYPE.containsKey(apiDataType)) {
      throw new IllegalArgumentException("No PrimitiveColumnTypes for apiDataType: " + apiDataType);
    }
    return BY_API_TYPE.get(apiDataType);
  }

  public static ColumnType fromApiTypeName(String apiTypeName) {
    return BY_API_TYPE_NAME.get(apiTypeName);
  }
}
