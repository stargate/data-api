package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.*;

/** Interface for primitive column types similar to what is defined in cassandra java driver. */
public enum PrimitiveColumnDesc implements ColumnDesc {
  ASCII(ApiDataTypeDefs.ASCII),
  BIGINT(ApiDataTypeDefs.BIGINT),
  BINARY(ApiDataTypeDefs.BINARY),
  BOOLEAN(ApiDataTypeDefs.BOOLEAN),
  COUNTER(ApiDataTypeDefs.COUNTER),
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
  TIMEUUID(ApiDataTypeDefs.TIMEUUID),
  TINYINT(ApiDataTypeDefs.TINYINT),
  UUID(ApiDataTypeDefs.UUID),
  VARINT(ApiDataTypeDefs.VARINT);

  public static final FromJsonFactory FROM_JSON_FACTORY = new FromJsonFactory();

  private static final Map<ApiDataType, PrimitiveColumnDesc> BY_API_TYPE = new HashMap<>();
  private static final Map<ApiTypeName, PrimitiveColumnDesc> BY_API_TYPE_NAME = new HashMap<>();

  private static final List<PrimitiveColumnDesc> allPrimitives;
  private static final List<ColumnDesc> allColumnDescs;

  static {
    allPrimitives = List.of(values());
    allColumnDescs = new ArrayList<>(allPrimitives);
    for (PrimitiveColumnDesc type : PrimitiveColumnDesc.values()) {
      BY_API_TYPE.put(type.apiDataType, type);
      BY_API_TYPE_NAME.put(type.apiDataType.typeName(), type);
    }
  }

  private final ApiDataType apiDataType;
  private final ApiSupportDesc apiSupportDesc;

  PrimitiveColumnDesc(ApiDataType apiDataType) {
    this.apiDataType = apiDataType;
    this.apiSupportDesc = ApiSupportDesc.from(apiDataType);
  }

  @Override
  public ApiTypeName typeName() {
    return apiDataType.typeName();
  }

  @Override
  public ApiSupportDesc apiSupport() {
    return apiSupportDesc;
  }

  public static List<PrimitiveColumnDesc> allPrimitives() {
    return allPrimitives;
  }

  public static List<ColumnDesc> allColumnDescs() {
    return allColumnDescs;
  }

  public static ColumnDesc fromApiDataType(ApiDataType apiDataType) {
    if (!apiDataType.isPrimitive()) {
      throw new IllegalArgumentException("Not a primitive type: " + apiDataType);
    }
    // sanity check that we have all the API types
    if (!BY_API_TYPE.containsKey(apiDataType)) {
      throw new IllegalArgumentException("No PrimitiveColumnDesc for apiDataType: " + apiDataType);
    }
    return BY_API_TYPE.get(apiDataType);
  }

  public static class FromJsonFactory {

    public Optional<ColumnDesc> create(ApiTypeName apiTypeName) {
      return Optional.ofNullable(BY_API_TYPE_NAME.get(apiTypeName));
    }
  }
}
