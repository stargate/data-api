package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.api.model.command.table.ApiMapComponent;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import java.util.HashMap;
import java.util.Map;

/**
 * ApiIndexFunction is a function that is applied in indexes on CQL collection type. <br>
 * Data API createIndex table command has defaults indexFunction for map/set/list, defaults are
 * entries(map), values(set), values(list). Data API does not support frozen map/set/list, so FULL
 * index creation on frozen column will also not be supported.
 */
public enum ApiIndexFunction {
  KEYS("keys"),
  VALUES("values"),
  ENTRIES("entries");

  private final String cqlFunction;

  private static final Map<String, ApiIndexFunction> FUNCTION_MAP;

  static {
    FUNCTION_MAP = new HashMap<>();
    for (ApiIndexFunction function : ApiIndexFunction.values()) {
      FUNCTION_MAP.put(function.cqlFunction.toLowerCase(), function);
    }
  }

  ApiIndexFunction(String cqlFunction) {
    this.cqlFunction = cqlFunction;
  }

  public static ApiIndexFunction fromCql(String cqlFunction)
      throws UnknownCqlIndexFunctionException {
    if (cqlFunction == null || !FUNCTION_MAP.containsKey(cqlFunction.toLowerCase())) {
      throw new UnknownCqlIndexFunctionException(cqlFunction);
    }
    return FUNCTION_MAP.get(cqlFunction.toLowerCase());
  }

  public static ApiIndexFunction fromApiMapComponent(ApiMapComponent apiMapComponent) {
    return switch (apiMapComponent) {
      case KEYS -> ApiIndexFunction.KEYS;
      case VALUES -> ApiIndexFunction.VALUES;
        // There is no $entries for map, null will be default to entries
      case null -> ApiIndexFunction.ENTRIES;
    };
  }

  public ApiMapComponent toApiMapComponent() {
    return switch (this) {
      case KEYS -> ApiMapComponent.KEYS;
      case VALUES -> ApiMapComponent.VALUES;
        // There is no $entries for map, null will be default to entries
      case ENTRIES -> null;
    };
  }
}
