package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import java.util.Map;

public enum ApiIndexFunction {
  KEYS("keys"),
  VALUES("values"),
  ENTRIES("entries");

  private final String cqlFunction;

  private static final Map<String, ApiIndexFunction> FUNCTION_MAP =
      Map.of(
          KEYS.cqlFunction, KEYS,
          VALUES.cqlFunction, VALUES,
          ENTRIES.cqlFunction, ENTRIES);

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
