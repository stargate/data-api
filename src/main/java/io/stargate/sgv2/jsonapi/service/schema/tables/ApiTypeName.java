package io.stargate.sgv2.jsonapi.service.schema.tables;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Names of the table column data types the API supports
 *
 * <p>NOTE: Use {@link #apiName} and {@link #fromApiName(String)} to convert to and from the name
 * used in requests and responses.
 */
public enum ApiTypeName {
  // Primitive Types
  ASCII("ascii"),
  BIGINT("bigint"),
  BINARY("blob"),
  BOOLEAN("boolean"),
  DATE("date"),
  DECIMAL("decimal"),
  DOUBLE("double"),
  DURATION("duration"),
  FLOAT("float"),
  INET("inet"),
  INT("int"),
  SMALLINT("smallint"),
  TEXT("text"),
  TIME("time"),
  TIMESTAMP("timestamp"),
  TINYINT("tinyint"),
  UUID("uuid"),
  TIMEUUID("timeuuid"),
  VARINT("varint"),

  // Special Types
  COUNTER("counter"),

  // Collection Types
  LIST("list"),
  MAP("map"),
  SET("set"),
  VECTOR("vector"),

  // UDT Types
  UDT("userDefined");

  private final String apiName;

  private static final List<ApiTypeName> all = List.of(values());
  private static final Map<String, ApiTypeName> BY_API_NAME = new HashMap<>();

  static {
    for (ApiTypeName type : ApiTypeName.values()) {
      BY_API_NAME.put(type.apiName, type);
    }
  }

  ApiTypeName(String apiName) {
    this.apiName = apiName;
  }

  /** The name to use for this type in requests and responses */
  public String apiName() {
    return apiName;
  }

  public static List<ApiTypeName> all() {
    return all;
  }

  public static Optional<ApiTypeName> fromApiName(String apiName) {
    return Optional.ofNullable(BY_API_NAME.get(apiName));
  }
}
