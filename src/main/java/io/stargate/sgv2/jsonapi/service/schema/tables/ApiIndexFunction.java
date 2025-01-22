package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import java.util.Map;

/**
 * ApiIndexFunction that can apply to collection columns map/set/list. <br>
 *
 * <pre>
 * Data API createIndex table command has defaults indexFunction for map/set/list
 * entries(map), values(set), values(list).
 * IndexFunction keys and values for map column are not supported.
 * Also, Data API does not support frozen map/set/list table creation, FULL index will not be supported.
 * </pre>
 */
public enum ApiIndexFunction {
  // KEYS("keys"),
  VALUES("values"),
  ENTRIES("entries");
  //  FULL("full");

  public final String cqlFunction;

  private static final Map<String, ApiIndexFunction> FUNCTION_MAP =
      Map.of(VALUES.cqlFunction, VALUES, ENTRIES.cqlFunction, ENTRIES);

  ApiIndexFunction(String cqlFunction) {
    this.cqlFunction = cqlFunction;
  }

  public static ApiIndexFunction fromCql(String cqlFunction)
      throws UnknownCqlIndexFunctionException {
    if (FUNCTION_MAP.containsKey(cqlFunction)) {
      return FUNCTION_MAP.get(cqlFunction);
    }
    throw new UnknownCqlIndexFunctionException(cqlFunction);
  }
}
