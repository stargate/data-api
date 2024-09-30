package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.exception.catchable.UnknownApiDataType;
import java.util.HashMap;
import java.util.Map;

/**
 * Names of the table column data types the API supports
 *
 * <p>NOTE: Use {@link #apiName} and {@link #fromApiName(String)} to convert to and from the name
 * used in requests and responses.
 */
public enum ApiDataType {
  ASCII("ascii"),
  BIGINT("bigint"),
  BINARY("blob"),
  BOOLEAN("boolean"),
  DATE("date"),
  DECIMAL("decimal"),
  DOUBLE("double"),
  DURATION("duration"),
  FLOAT("float"),
  INT("int"),
  SMALLINT("smallint"),
  TEXT("text"),
  TIME("time"),
  TIMESTAMP("timestamp"),
  TINYINT("tinyint"),
  VARINT("varint");

  private static final Map<String, ApiDataType> TYPE_BY_API_NAME = new HashMap<>();

  static {
    // cannot access the static map from the constructor
    for (ApiDataType type : ApiDataType.values()) {
      TYPE_BY_API_NAME.put(type.apiName, type);
    }
  }

  private final String apiName;

  ApiDataType(String apiName) {
    this.apiName = apiName;
  }

  /** The name to use for this type in requests and responses */
  public String getApiName() {
    return apiName;
  }

  /**
   * Get the {@link ApiDataType} for the given apiName
   *
   * @throws UnknownApiDataType if the name is not recognized
   */
  public static ApiDataType fromApiName(String apiName) throws UnknownApiDataType {
    if (TYPE_BY_API_NAME.containsKey(apiName)) {
      return TYPE_BY_API_NAME.get(apiName);
    }
    throw new UnknownApiDataType(apiName);
  }
}
