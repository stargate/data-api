package io.stargate.sgv2.jsonapi.service.schema.tables;

/**
 * Names of the table column data types the API supports
 *
 * <p>NOTE: Use {@link #apiName} and {@link #fromApiName(String)} to convert to and from the name
 * used in requests and responses.
 */
public enum ApiDataTypeName {
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

  // Complex Types
  LIST("list"),
  MAP("map"),
  SET("set"),
  VECTOR("vector");

  private final String apiName;

  ApiDataTypeName(String apiName) {
    this.apiName = apiName;
  }

  /** The name to use for this type in requests and responses */
  public String getApiName() {
    return apiName;
  }
}
