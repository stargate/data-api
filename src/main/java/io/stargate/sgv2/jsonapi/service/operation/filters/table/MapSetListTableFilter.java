package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnMetadata;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.internal.querybuilder.DefaultRaw;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ArrayComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.table.MapSetListComponent;
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import java.util.List;

/** Table filter against the map/set/list column. */
public class MapSetListTableFilter extends TableFilter {

  /** The operator that will be used to filter the map/set/list. */
  private final Operator operator;

  /** For all the {@link Operator} that can apply to map/set/list, a list of values is required. */
  private final List<Object> arrayValue;

  /** The map/set/list component that will be filtered against. */
  private final MapSetListFilterComponent mapSetListFilterComponent;

  public MapSetListTableFilter(
      Operator operator,
      String path,
      List<Object> arrayValue,
      MapSetListFilterComponent mapSetListFilterComponent) {
    super(path, null);
    this.operator = operator;
    this.arrayValue = arrayValue;
    this.mapSetListFilterComponent = mapSetListFilterComponent;
  }

  /** The map/set/list component that will be filtered against. */
  public enum MapSetListFilterComponent {
    LIST_VALUE,
    SET_VALUE,
    MAP_ENTRY,
    MAP_KEY,
    MAP_VALUE;

    public static MapSetListFilterComponent from(MapSetListComponent component) {
      return switch (component) {
        case LIST_VALUE -> LIST_VALUE;
        case SET_VALUE -> SET_VALUE;
        case MAP_ENTRY -> MAP_ENTRY;
        case MAP_KEY -> MAP_KEY;
        case MAP_VALUE -> MAP_VALUE;
      };
    }
  }

  /**
   * Following operators are supported for map. This looks like a duplicate for {@link
   * FilterOperator}, but it is made on purpose to avoid coupling of the API and Operation.
   */
  public enum Operator {
    IN,
    NIN,
    ALL,
    /**
     * NOT_ANY is used internally to support $not operation to negate $all. It is not supported in
     * the API level.
     */
    NOT_ANY;

    public static Operator from(FilterOperator ApiFilterOperator) {
      return switch (ApiFilterOperator) {
        case ValueComparisonOperator valueComparisonOperator ->
            switch (valueComparisonOperator) {
              case IN -> IN;
              case NIN -> NIN;
              default ->
                  throw new IllegalArgumentException(
                      "Unsupported ApiFilterOperator for map/set/list column: "
                          + ApiFilterOperator);
            };
        case ArrayComparisonOperator arrayComparisonOperator ->
            switch (arrayComparisonOperator) {
              case ALL -> ALL;
              case NOTANY -> NOT_ANY;
              default ->
                  throw new IllegalArgumentException(
                      "Unsupported ApiFilterOperator for map/set/list column: "
                          + ApiFilterOperator);
            };
        case null, default ->
            throw new IllegalArgumentException(
                "Unsupported ApiFilterOperator for map/set/list column: " + ApiFilterOperator);
      };
    }
  }

  /**
   * Apply the table filter(against a list/set table column), return the {@link Relation} to build
   * parts of {@link OngoingWhereClause}.
   */
  private Relation applyAgainstListSet(
      List<Object> positionalValues, CollectionApiDataType<?> listOrSetApiType)
      throws MissingJSONCodecException, ToCQLCodecException {

    // feed each value to the codec and append to statement positionalValues
    for (Object singleValue : arrayValue) {
      var codec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              listOrSetApiType.getValueType().cqlType(), singleValue);
      positionalValues.add(codec.toCQL(singleValue));

      // TODO, need Driver logic OR/AND ability, currently works for single element
    }
    // With the and/or ability, then we can logically connect the Relation here
    return Relation.column(getPathAsCqlIdentifier())
        .build(operatorToPredicate().getSpaceWrappedCql(), bindMarker());
  }

  /**
   * Apply the table filter(against a map table column), return the {@link Relation} to build parts
   * of {@link OngoingWhereClause}.
   */
  private Relation applyAgainstMap(List<Object> positionalValues, ApiMapType apiMapType)
      throws MissingJSONCodecException, ToCQLCodecException {

    // filter against map entry
    // sample cql: mapColumn['a'] = 'a1' OR mapColumn['b'] = 'b1'
    if (mapSetListFilterComponent == MapSetListFilterComponent.MAP_ENTRY) {
      return applyAgainstMapEntry(positionalValues, apiMapType);
    }

    // filter against map key/value
    // sample cql: mapColumn contains key 'a' OR mapColumn contains key 'b'
    // sample cql: mapColumn contains 'a' OR mapColumn contains 'b'
    return applyAgainstMapKeyOrValue(positionalValues, apiMapType);
  }

  /** Apply the table filter(against the entry of map column). */
  private Relation applyAgainstMapEntry(List<Object> positionalValues, ApiMapType apiMapType)
      throws MissingJSONCodecException, ToCQLCodecException {

    for (Object singleEntry : arrayValue) {
      List<Object> mapKeyAndValue = (List<Object>) singleEntry;
      var keyCodec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              apiMapType.getKeyType().cqlType(), mapKeyAndValue.get(0));
      var valueCodec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              apiMapType.getValueType().cqlType(), mapKeyAndValue.get(1));
      positionalValues.add(keyCodec.toCQL(mapKeyAndValue.get(0)));
      positionalValues.add(valueCodec.toCQL(mapKeyAndValue.get(1)));
      // here and/or relation together

      // TODO, need Driver logic OR/AND ability, currently works for single element

    }
    return new DefaultRaw("%s[?] = ?".formatted(path));
  }

  /** Apply the table filter(against the key/value of map column). */
  private Relation applyAgainstMapKeyOrValue(List<Object> positionalValues, ApiMapType apiMapType)
      throws MissingJSONCodecException, ToCQLCodecException {

    DataType keyOrValueType =
        mapSetListFilterComponent == MapSetListFilterComponent.MAP_KEY
            ? apiMapType.getKeyType().cqlType()
            : apiMapType.getValueType().cqlType();
    for (Object singleKeyOrValue : arrayValue) {
      var keyOrValueCodec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(keyOrValueType, singleKeyOrValue);
      positionalValues.add(keyOrValueCodec.toCQL(singleKeyOrValue));
      // TODO, need Driver logic OR/AND ability, currently works for single element
    }
    return Relation.column(getPathAsCqlIdentifier())
        .build(operatorToPredicate().getSpaceWrappedCql(), bindMarker());
  }

  /**
   * Method to convert map/set/list filter operator the {@link BuiltConditionPredicate}.
   *
   * <p>E.G. (Note, list and set are identical across all operator and predicate).
   *
   * <ul>
   *   <li>list({@link Operator#IN})-> <code>
   *       where listColumn contains 'a' OR listColumn contains 'b'</code</li>
   *   <li>list({@link Operator#NIN})-> <code>
   *       where listColumn not contains 'a' AND listColumn not contains 'b'</code</li>
   *   <li>list({@link Operator#ALL})-> <code>
   *       where listColumn contains 'a' AND listColumn contains 'b' </code</li>
   *   <li>list({@link Operator#NOT_ANY})-> <code>
   *       where listColumn not contains 'a' OR listColumn not contains 'b'</code</li>
   *   <li>map keys ({@link Operator#IN}-> <code>
   *       where mapColumn contains key 'a' OR mapColumn contains key 'b'</code>
   *   <li>map keys ({@link Operator#NIN}-> <code>
   *       where mapColumn not contains key 'a' AND mapColumn not contains key 'b'</code>
   *   <li>map keys ({@link Operator#ALL}-> <code>
   *       where mapColumn contains key 'a' AND mapColumn contains key 'b'</code>
   *   <li>map keys ({@link Operator#NOT_ANY}-> <code>
   *       where mapColumn not contains key 'a' OR mapColumn not contains key 'b'</code>
   *   <li>map values ({@link Operator#IN}-> <code>
   *       where mapColumn contains 'a' OR mapColumn contains 'b'</code>
   *   <li>map values ({@link Operator#NIN}-> <code>
   *       where mapColumn not contains 'a' AND mapColumn not contains 'b'</code>
   *   <li>map values ({@link Operator#ALL}-> <code>
   *       where mapColumn contains 'a' AND mapColumn contains 'b'</code>
   *   <li>map values ({@link Operator#NOT_ANY}-> <code>
   *       where mapColumn not contains 'a' OR mapColumn not contains 'b'</code>
   *   <li>map entries ({@link Operator#IN}-> <code>
   *       where mapColumn['a'] = 'a1' OR mapColumn['b'] = 'b1'</code>
   *   <li>map entries ({@link Operator#NIN}-> <code>
   *       where mapColumn['a'] != 'a1' AND mapColumn['b'] != 'b1'</code>
   *   <li>map entries ({@link Operator#ALL}-> <code>
   *       where mapColumn['a'] = 'a1' AND mapColumn['b'] = 'b1'</code>
   *   <li>map entries ({@link Operator#NOT_ANY}-> <code>
   *       where mapColumn['a'] != 'a1' OR mapColumn['b'] != 'b1'</code>
   * </ul>
   */
  private BuiltConditionPredicate operatorToPredicate() {
    return switch (mapSetListFilterComponent) {
      case LIST_VALUE, SET_VALUE, MAP_VALUE ->
          switch (operator) {
            case IN, ALL -> BuiltConditionPredicate.CONTAINS;
            case NIN, NOT_ANY -> BuiltConditionPredicate.NOT_CONTAINS;
          };
      case MAP_KEY ->
          switch (operator) {
            case IN, ALL -> BuiltConditionPredicate.CONTAINS_KEY;
            case NIN, NOT_ANY -> BuiltConditionPredicate.NOT_CONTAINS_KEY;
          };
      case MAP_ENTRY ->
          switch (operator) {
            case IN, ALL -> BuiltConditionPredicate.EQ;
            case NIN, NOT_ANY -> BuiltConditionPredicate.NEQ;
          };
    };
  }

  @Override
  public <StmtT extends OngoingWhereClause<StmtT>> StmtT apply(
      TableSchemaObject table, StmtT ongoingWhereClause, List<Object> positionalValues) {

    // get the column and its DataType
    var column = getPathAsCqlIdentifier();
    var columnDef = table.apiTableDef().allColumns().get(column);
    if (columnDef == null) {
      // this won't happen, since this tableFilter was deserialized knowing the schema.
      throw FilterException.Code.UNKNOWN_TABLE_COLUMNS.get(
          errVars(
              table,
              map -> {
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(table.tableMetadata().getColumns().values()));
                map.put("unknownColumns", path);
              }));
    }

    Relation relation = null;

    try {
      relation =
          switch (columnDef.type()) {
            case ApiListType apiListType -> applyAgainstListSet(positionalValues, apiListType);
            case ApiSetType apiSetType -> applyAgainstListSet(positionalValues, apiSetType);
            case ApiMapType apiMapType -> applyAgainstMap(positionalValues, apiMapType);
            default ->
                throw new IllegalArgumentException(
                    "Unsupported ApiDataType for map/set/list column: " + columnDef.type());
          };
    } catch (MissingJSONCodecException e) {
      throw DocumentException.Code.UNSUPPORTED_COLUMN_TYPES.get(
          errVars(
              table,
              map -> {
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(table.tableMetadata().getColumns().values()));
                map.put("unsupportedColumns", path);
              }));
    } catch (ToCQLCodecException e) {
      throw FilterException.Code.INVALID_FILTER_COLUMN_VALUES.get(
          errVars(
              table,
              map -> {
                map.put(
                    "allColumns",
                    errFmtColumnMetadata(table.tableMetadata().getColumns().values()));
                map.put("invalidColumn", path);
                map.put(
                    "columnType", table.tableMetadata().getColumn(path).get().getType().toString());
              }));
    }

    return ongoingWhereClause.where(relation);
  }

  /**
   * Column map/set/list can not be used as partitionKey in C* table, so this override won't be
   * actually called.
   */
  @Override
  public boolean filterIsExactMatch() {
    return false;
  }

  /** Not filtering contiguous slice of ordered values, e.g. >, >=, <, <=. */
  @Override
  public boolean filterIsSlice() {
    return false;
  }

  /**
   * This is an override method from DBFilterBase interface. When we migrate collection filter path
   * into the new design, this method may need to be implemented.
   *
   * @return BuiltCondition
   */
  @Override
  public BuiltCondition get() {
    throw new UnsupportedOperationException(
        "Not supported - will be modified when we migrate collections filters java driver");
  }
}
