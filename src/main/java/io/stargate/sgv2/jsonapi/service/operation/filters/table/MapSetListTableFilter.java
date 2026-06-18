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
import io.stargate.sgv2.jsonapi.exception.DocumentException;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.checked.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.exception.checked.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.DefaultSubConditionRelation;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistries;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.schema.tables.*;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.List;

/** Table filter against the map/set/list column. */
public class MapSetListTableFilter extends TableFilter {

  /** The operator that will be used to filter the map/set/list. */
  private final Operator operator;

  /** For all the {@link Operator} that can apply to map/set/list, a list of values is required. */
  private final List<Object> matchValues;

  /** The map/set/list component that will be filtered against. */
  private final MapSetListFilterComponent filterComponent;

  public MapSetListTableFilter(
      Operator operator,
      String path,
      List<Object> matchValues,
      MapSetListFilterComponent filterComponent) {
    super(path, null);
    this.operator = operator;
    this.matchValues = matchValues;
    this.filterComponent = filterComponent;
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

  public MapSetListFilterComponent getFilterComponent() {
    return filterComponent;
  }

  @Override
  public Relation apply(TableSchemaObject table, List<Object> positionalValues) {

    // get the column and its DataType
    // TODO: the column existing should be checked before getting this far.

    var columnDef = table.apiTableDef().allColumns().get(getPathAsCqlIdentifier());
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
            case ApiListType apiListType -> applyToListSet(table, positionalValues, apiListType);
            case ApiSetType apiSetType -> applyToListSet(table, positionalValues, apiSetType);
            case ApiMapType apiMapType -> applyToMap(table, positionalValues, apiMapType);
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
                    "columnType",
                    table
                        .tableMetadata()
                        .getColumn(getPathAsCqlIdentifier())
                        .get()
                        .getType()
                        .toString());
              }));
    }

    return relation;
  }

  /**
   * Apply the table filter(against a list/set table column), return the {@link Relation} to build
   * parts of {@link OngoingWhereClause}.
   */
  private Relation applyToListSet(
      TableSchemaObject tableSchemaObject,
      List<Object> positionalValues,
      CollectionApiDataType<?> listOrSetApiType)
      throws MissingJSONCodecException, ToCQLCodecException {

    DefaultSubConditionRelation ongoingWhereClause = DefaultSubConditionRelation.subCondition();

    boolean isFirst = true;
    for (Object singleValue : matchValues) {

      var codec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              tableSchemaObject.tableMetadata(),
              getPathAsCqlIdentifier(),
              listOrSetApiType.getValueType().cqlType(),
              singleValue);

      positionalValues.add(codec.toCQL(singleValue));
      ongoingWhereClause = addSingleSubRelation(ongoingWhereClause, isFirst);
      isFirst = false;
    }
    return ongoingWhereClause;
  }

  /**
   * Apply the table filter(against a map table column), return the {@link Relation} to build parts
   * of {@link OngoingWhereClause}.
   */
  private Relation applyToMap(
      TableSchemaObject tableSchemaObject, List<Object> positionalValues, ApiMapType apiMapType)
      throws MissingJSONCodecException, ToCQLCodecException {

    // filter against map entry
    // sample cql: mapColumn['a'] = 'a1' OR mapColumn['b'] = 'b1'
    if (filterComponent == MapSetListFilterComponent.MAP_ENTRY) {
      return applyAgainstMapEntry(tableSchemaObject, positionalValues, apiMapType);
    }

    // filter against map key/value
    // sample cql: mapColumn contains key 'a' OR mapColumn contains key 'b'
    // sample cql: mapColumn contains 'a' OR mapColumn contains 'b'
    return applyToMapKeyOrValue(tableSchemaObject, positionalValues, apiMapType);
  }

  /** Apply the table filter(against the entry of map column). */
  private Relation applyAgainstMapEntry(
      TableSchemaObject tableSchemaObject, List<Object> positionalValues, ApiMapType apiMapType)
      throws MissingJSONCodecException, ToCQLCodecException {

    var ongingWhereClause = DefaultSubConditionRelation.subCondition();

    boolean isFirst = true;
    for (List<Object> entryTuple : uncheckedMatchValuesAsTuple()) {

      // TODO: - should we check the size here ?
      // remember, the keys may not be strings they can be any type now.
      var key = entryTuple.get(0);
      var value = entryTuple.get(1);

      var keyCodec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              tableSchemaObject.tableMetadata(),
              getPathAsCqlIdentifier(),
              apiMapType.getKeyType().cqlType(),
              key);
      var valueCodec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              tableSchemaObject.tableMetadata(),
              getPathAsCqlIdentifier(),
              apiMapType.getValueType().cqlType(),
              value);

      positionalValues.add(keyCodec.toCQL(key));
      positionalValues.add(valueCodec.toCQL(value));
      ongingWhereClause = addSingleSubRelation(ongingWhereClause, isFirst);
      isFirst = false;
    }
    return ongingWhereClause;
  }

  /**
   * Convert the matchValues to a list of tuples, where each tuple is a list of two objects, the key
   * and value of the map entry. Does not check if they are only two elements in the list.
   *
   * <p>This is used when the filter component is {@link MapSetListFilterComponent#MAP_ENTRY}.
   */
  @SuppressWarnings("unchecked")
  private List<List<Object>> uncheckedMatchValuesAsTuple() {
    return matchValues.stream().map(value -> (List<Object>) value).toList();
  }

  /** Apply the table filter(against the key/value of map column). */
  private Relation applyToMapKeyOrValue(
      TableSchemaObject tableSchemaObject, List<Object> positionalValues, ApiMapType apiMapType)
      throws MissingJSONCodecException, ToCQLCodecException {

    DataType keyOrValueType =
        filterComponent == MapSetListFilterComponent.MAP_KEY
            ? apiMapType.getKeyType().cqlType()
            : apiMapType.getValueType().cqlType();

    var ongoingWhereClause = DefaultSubConditionRelation.subCondition();
    boolean isFirst = true;

    for (Object singleKeyOrValue : matchValues) {

      var codec =
          JSONCodecRegistries.DEFAULT_REGISTRY.codecToCQL(
              tableSchemaObject.tableMetadata(),
              getPathAsCqlIdentifier(),
              keyOrValueType,
              singleKeyOrValue);

      positionalValues.add(codec.toCQL(singleKeyOrValue));
      // build up the logical relation according to the operator applied to the map/set/list.
      ongoingWhereClause = addSingleSubRelation(ongoingWhereClause, isFirst);
      isFirst = false;
    }
    return ongoingWhereClause;
  }

  /**
   * Adds to a relationship / condition to the ongoing where clause, using `?` bind markers , so the
   * caller is responsible for adding the positional values.
   *
   * <ul>
   *   <li>list({@link Operator#IN})-> <code>
   *       where listColumn contains 'a' OR listColumn contains 'b'</code</li>
   *   <li>list({@link Operator#NIN})-> <code>
   *       where listColumn not contains 'a' AND listColumn not contains 'b'</code</li>
   *   <li>map entries ({@link Operator#IN}-> <code>
   *       where mapColumn['a'] = 'a1' OR mapColumn['b'] = 'b1'</code>
   *   <li>map entries ({@link Operator#NIN}-> <code>
   *       where mapColumn['a'] != 'a1' AND mapColumn['b'] != 'b1'</code>
   * </ul>
   *
   * @param ongoingWhereClause The ongoing where clause to append the relation to.
   * @param isFirst true is the first in a series of conditions, see {@link
   *     SubLogicRelation#appendRelation}
   * @return the {@link DefaultSubConditionRelation} used returned from the query builder, caller
   *     should use this for the next filter condition.
   */
  private DefaultSubConditionRelation addSingleSubRelation(
      DefaultSubConditionRelation ongoingWhereClause, boolean isFirst) {

    // the driver query builder cannot support map entry filter, so we have to build it manually
    // E.G. map_column[?] = ?
    // note, since this is raw cql, need to double quote to adapt with quoted identifier
    // E.G. "map_column"[?] = ?
    Relation relation =
        (filterComponent == MapSetListFilterComponent.MAP_ENTRY)
            ? new DefaultRaw(
                "\"%s\"[?]%s?"
                    .formatted(
                        getPathAsCqlIdentifier().asInternal(),
                        operator.cqlPredicate(filterComponent)))
            : Relation.column(getPathAsCqlIdentifier())
                .build(operator.cqlPredicate(filterComponent).getCql(), bindMarker());

    return operator.subLogicRelation.appendRelation(ongoingWhereClause, relation, isFirst);
  }

  /**
   * Following operators are supported for map. This looks like a duplicate for {@link
   * FilterOperator}, but it is made on purpose to avoid coupling of the API and Operation.
   */
  public enum Operator {
    IN(SubLogicRelation.OR),
    NIN(SubLogicRelation.AND),
    ALL(SubLogicRelation.AND),
    /**
     * NOT_ANY is used internally to support $not operation to negate $all. It is not supported in
     * the API level.
     */
    NOT_ANY(SubLogicRelation.OR);

    public final SubLogicRelation subLogicRelation;

    Operator(SubLogicRelation subLogicRelation) {
      this.subLogicRelation = subLogicRelation;
    }

    public static Operator from(FilterOperator apiFilterOperator) {
      return switch (apiFilterOperator) {
        case ValueComparisonOperator valueComparisonOperator ->
            switch (valueComparisonOperator) {
              case IN -> IN;
              case NIN -> NIN;
              default -> throw unsupportedOperator(apiFilterOperator);
            };
        case ArrayComparisonOperator arrayComparisonOperator ->
            switch (arrayComparisonOperator) {
              case ALL -> ALL;
              case NOTANY -> NOT_ANY;
              default -> throw unsupportedOperator(apiFilterOperator);
            };
        default -> throw unsupportedOperator(apiFilterOperator);
      };
    }

    /**
     * Gets the {@link BuiltConditionPredicate} for this operator when applied to the specified
     * component of map/set/list.
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
    private BuiltConditionPredicate cqlPredicate(
        MapSetListFilterComponent mapSetListFilterComponent) {
      return switch (this) {
        case IN, ALL ->
            switch (mapSetListFilterComponent) {
              case LIST_VALUE, SET_VALUE, MAP_VALUE -> BuiltConditionPredicate.CONTAINS;
              case MAP_KEY -> BuiltConditionPredicate.CONTAINS_KEY;
              case MAP_ENTRY -> BuiltConditionPredicate.EQ;
            };
        case NIN, NOT_ANY ->
            switch (mapSetListFilterComponent) {
              case LIST_VALUE, SET_VALUE, MAP_VALUE -> BuiltConditionPredicate.NOT_CONTAINS;
              case MAP_KEY -> BuiltConditionPredicate.NOT_CONTAINS_KEY;
              case MAP_ENTRY -> BuiltConditionPredicate.NEQ;
            };
      };
    }

    private static IllegalArgumentException unsupportedOperator(FilterOperator apiFilterOperator) {
      return new IllegalArgumentException(
          "Unsupported ApiFilterOperator for map/set/list column: " + apiFilterOperator);
    }
  }

  /**
   * One MapSetListTableFilter could resolve as multiple predicates grouped together by AND/OR
   * according to different Operators. E.G.
   *
   * <ul>
   *   <li>list({@link Operator#IN})-> <code>
   *       where listColumn contains 'a' OR listColumn contains 'b'</code</li>
   *   <li>list({@link Operator#NIN})-> <code>
   *       where listColumn not contains 'a' AND listColumn not contains 'b'</code</li>
   *   <li>list({@link Operator#ALL})-> <code>
   *       where listColumn contains 'a' AND listColumn contains 'b'</code</li>
   *   <li>list({@link Operator#NOT_ANY})-> <code>
   *       where listColumn not contains 'a' OR listColumn not contains 'b'</code</li>
   * </ul>
   */
  public enum SubLogicRelation {
    AND,
    OR;

    public DefaultSubConditionRelation appendRelation(
        DefaultSubConditionRelation ongoingWhere, Relation subRelation, boolean isFirst) {
      return switch (this) {
        case AND ->
            isFirst ? ongoingWhere.where(subRelation) : ongoingWhere.and().where(subRelation);
        case OR -> isFirst ? ongoingWhere.where(subRelation) : ongoingWhere.or().where(subRelation);
      };
    }
    ;
  }
}
