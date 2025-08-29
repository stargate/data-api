package io.stargate.sgv2.jsonapi.service.schema.tables;

import java.util.*;

/**
 * Names of the table column data types the API supports
 *
 * <p>NOTE: Use {@link #apiName} and {@link #fromApiName(String)} to convert to and from the name
 * used in requests and responses.
 */
public enum ApiTypeName {
  // Primitive Types
  ASCII("ascii", true, false),
  BIGINT("bigint", true, false),
  BINARY("blob", true, false),
  BOOLEAN("boolean", true, false),
  DATE("date", true, false),
  DECIMAL("decimal", true, false),
  DOUBLE("double", true, false),
  DURATION("duration", true, false),
  FLOAT("float", true, false),
  INET("inet", true, false),
  INT("int", true, false),
  SMALLINT("smallint", true, false),
  TEXT("text", true, false),
  TIME("time", true, false),
  TIMESTAMP("timestamp", true, false),
  TINYINT("tinyint", true, false),
  UUID("uuid", true, false),
  TIMEUUID("timeuuid", true, false),
  VARINT("varint", true, false),

  // Special Types
  // Counter is still considered primitive, but it is a special type
  COUNTER("counter", true, false),

  // Collection Types
  LIST("list", false, true),
  MAP("map", false, true),
  SET("set", false, true),
  // vector is a container of floats
  VECTOR("vector", false, true),

  // UDT Types
  UDT("userDefined", false, false);

  /** Comparator to sort ApiTypeName by their {@link #apiName()} */
  public static final Comparator<ApiTypeName> COMPARATOR =
      Comparator.comparing(ApiTypeName::apiName);

  private final String apiName;
  private final boolean isPrimitive;
  private final boolean isContainer;

  private static final List<ApiTypeName> all = List.of(values());
  private static final Map<String, ApiTypeName> BY_API_NAME = new HashMap<>();

  static {
    for (ApiTypeName type : ApiTypeName.values()) {
      BY_API_NAME.put(type.apiName, type);
    }
  }

  ApiTypeName(String apiName, boolean isPrimitive, boolean isContainer) {
    this.apiName = apiName;
    this.isPrimitive = isPrimitive;
    this.isContainer = isContainer;
  }

  /** The name to use for this type in requests and responses */
  public String apiName() {
    return apiName;
  }

  public boolean isPrimitive() {
    return isPrimitive;
  }

  public boolean isContainer() {
    return isContainer;
  }

  public static List<ApiTypeName> all() {
    return all;
  }

  public static Optional<ApiTypeName> fromApiName(String apiName) {
    return Optional.ofNullable(BY_API_NAME.get(apiName));
  }
}
