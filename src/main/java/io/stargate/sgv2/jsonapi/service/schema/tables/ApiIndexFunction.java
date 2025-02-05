package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
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

  public final String cqlFunction;

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

  public static ApiIndexFunction fromDollarCommand(String indexFunctionWithDollarSign) {
    return switch (indexFunctionWithDollarSign) {
      case TableDescConstants.CollectionTypeComponent.keys -> KEYS;
      case TableDescConstants.CollectionTypeComponent.values -> VALUES;
      case TableDescConstants.CollectionTypeComponent.entries -> ENTRIES;
      default -> throw SchemaException.Code.INVALID_FORMAT_FOR_INDEX_CREATION_COLUMN.get();
    };
  }
}
