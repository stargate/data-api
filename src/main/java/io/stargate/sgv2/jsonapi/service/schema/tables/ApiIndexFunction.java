package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToCQL;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndex;
import com.datastax.oss.driver.api.querybuilder.schema.CreateIndexOnTable;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapComponentDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnknownCqlIndexFunctionException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * ApiIndexFunction is a function that is applied in indexes on CQL collection type.
 *
 * <p>Data API createIndex table command has defaults indexFunction for <code>map</code>/<code>set
 * </code>/<code>list</code>, defaults are <code>entries(map)</code>, <code>values(set)</code>,
 * <code>values(list)</code>. Data API does not support frozen <code>map</code>/<code>set</code>/
 * <code>list</code>, so FULL index creation on frozen column will also not be supported.
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

  public String cqlFunction() {
    return cqlFunction;
  }

  public static String toTargetString(ApiIndexFunction indexFunction, CqlIdentifier targetColumn) {
    Objects.requireNonNull(targetColumn, "targetColumn cannot be null");
    return indexFunction == null
        ? cqlIdentifierToCQL(targetColumn)
        : indexFunction.cqlFunction() + "(" + cqlIdentifierToCQL(targetColumn) + ")";
  }

  public static CreateIndex addTo(
      CreateIndexOnTable createIndexOnTable,
      ApiIndexFunction indexFunction,
      CqlIdentifier targetColumn) {
    Objects.requireNonNull(createIndexOnTable, "createIndexOnTable cannot be null");
    Objects.requireNonNull(targetColumn, "targetColumn cannot be null");

    return switch (indexFunction) {
      case KEYS -> createIndexOnTable.andColumnKeys(targetColumn);
      case VALUES -> createIndexOnTable.andColumnValues(targetColumn);
      case ENTRIES -> createIndexOnTable.andColumnEntries(targetColumn);
      case null -> createIndexOnTable.andColumn(targetColumn);
    };
  }

  public static ApiIndexFunction fromCql(String cqlFunction)
      throws UnknownCqlIndexFunctionException {
    if (cqlFunction == null || !FUNCTION_MAP.containsKey(cqlFunction.toLowerCase())) {
      throw new UnknownCqlIndexFunctionException(cqlFunction);
    }
    return FUNCTION_MAP.get(cqlFunction.toLowerCase());
  }

  public static ApiIndexFunction fromMapComponentDesc(MapComponentDesc mapComponentDesc) {
    return switch (mapComponentDesc) {
      case KEYS -> ApiIndexFunction.KEYS;
      case VALUES -> ApiIndexFunction.VALUES;
        // There is no $entries for map, null will be default to entries
      case null -> ApiIndexFunction.ENTRIES;
    };
  }

  public MapComponentDesc toApiMapComponent() {
    return switch (this) {
      case KEYS -> MapComponentDesc.KEYS;
      case VALUES -> MapComponentDesc.VALUES;
        // There is no $entries for map, null will be default to entries
      case ENTRIES -> null;
    };
  }
}
