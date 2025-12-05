package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapComponentDesc;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListFilterComponent;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.*;

public class TableFilterClauseBuilder extends FilterClauseBuilder<TableSchemaObject> {

  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Filter operator that are allowed for map/set/list column. TODO, needs a better way to do it.
   * Could create another implementation for {@link FilterOperator}, but the actual meaning of
   * different implementation has been abused and is confusing already.
   */
  private static Set<String> SUPPORTED_MAP_SET_LIST_OPERATORS =
      Set.of(
          ValueComparisonOperator.IN.getOperator(),
          ValueComparisonOperator.NIN.getOperator(),
          ArrayComparisonOperator.ALL.getOperator());

  private static String ERR_MESSAGE_SUPPORTED_MAP_SET_LIST_OPERATORS =
      String.join(
          ", ",
          SUPPORTED_MAP_SET_LIST_OPERATORS.stream()
              .sorted(Comparator.naturalOrder())
              .map(x -> "'" + x + "'")
              .toList());

  public TableFilterClauseBuilder(TableSchemaObject schema) {
    super(schema);
  }

  // Tables do not have fixed "_id" as THE document id
  @Override
  protected boolean isDocId(String path) {
    return false;
  }

  @Override
  protected FilterClause validateAndBuild(LogicalExpression implicitAnd) {
    return new FilterClause(implicitAnd);
  }

  @Override
  protected String validateFilterClausePath(String path, FilterOperator operator) {
    return path;
  }

  /**
   * Create the list of ComparisonExpressions from a single path entry. (NOTE, this is builder for
   * tables. So the path also refers to the column name.)
   *
   * <p>Single entry representation: Key(path), Value(jsonNode). E.G.
   *
   * <ul>
   *   <li><code>{"nameColumn": {"$eq": "Monkey"}}</code>
   *   <li><code>{"ageColumn": {"$gt": 10, "$lt": 50}}</code>
   *   <li><code>{"mapColumn": {"$keys": {"$in" : ["key1", "key2"]}}}</code> KEYS will be detected.
   *   <li><code>{"listColumn": {"$all" : ["value1", "value2"]}}</code>
   * </ul>
   */
  protected List<ComparisonExpression> buildFromPathEntry(Map.Entry<String, JsonNode> entry) {
    // if path entry is filtering against the map/set/list column
    String path = entry.getKey();
    var filterOnCollectionColumn = filterMapSetListColumn(path, entry.getValue());
    if (!filterOnCollectionColumn.isEmpty()) {
      return filterOnCollectionColumn;
    }

    // the shared logic for both Collection and Table.
    return buildFromPathEntryCommon(entry);
  }

  /**
   * Check if the path is against a map/set/list column in the tableSchema. If so, we need to build
   * filter operation for it.
   */
  private List<ComparisonExpression> filterMapSetListColumn(String path, JsonNode pathValue) {

    // Find the map/set/list column first.
    var mapSetListColumn =
        schema.apiTableDef().allColumns().get(CqlIdentifierUtil.cqlIdentifierFromUserInput(path));
    if (mapSetListColumn == null || !mapSetListColumn.type().isContainer()) {
      return List.of();
    }
    return switch (mapSetListColumn.type().typeName()) {
      case MAP -> filterMapColumn(path, pathValue);
      case LIST, SET -> filterSetListColumn(path, pathValue, mapSetListColumn.type().typeName());
      default -> List.of();
    };
  }

  /**
   * To filter against a map column, we need to check the mapComponent and the operator.
   *
   * <p>allowed filter operators: <code>$in</code>, <code>$nin</code>, <code>$all
   * </code>.
   *
   * <p>allowed mapComponent: <code>$keys</code>, <code>$values</code>, <code>
   * entry(no need to specified)</code>.
   *
   * <p>JsonNode pathValue E.G.
   *
   * <ul>
   *   <li><code>{"$keys": {"$nin" : ["key1", "key2"]}, "$values": {"$in": ["value1","value2"]}
   *       </code>
   *   <li><code>{"$keys": {"$nin" : ["key1", "key2"], "$in": ["key3", "key4"]}}</code>
   *   <li><code>{"$in": [["key1", "value1"], ["key2", "value2"]]}</code>
   * </ul>
   */
  private List<ComparisonExpression> filterMapColumn(String columnName, JsonNode pathValue) {

    if (!pathValue.isObject()) {
      throw FilterException.Code.INVALID_MAP_SET_LIST_FILTER.get(
          errVars(
              schema,
              map -> {
                map.put(
                    "detailedReason",
                    "The value for the column '%s' is not a valid JSON object"
                        .formatted(columnName));
              }));
    }

    List<ComparisonExpression> comparisonExpressions = new ArrayList<>();
    var fieldsIter = pathValue.fields();
    // iterate through the filter fields.
    while (fieldsIter.hasNext()) {
      Map.Entry<String, JsonNode> jsonNodeEntry = fieldsIter.next();
      // nodeEntryKey can be mapComponent $keys/$values OR operator like $in/$nin/$all
      // so jsonNodeEntry like {$keys": {"$nin" : ["key1", "key2"]}}
      // or jsonNodeEntry like {"$in": [["key1", "value1"], ["key2", "value2"]]}

      final String nodeEntryKey = jsonNodeEntry.getKey();
      JsonNode nodeEntryValue = jsonNodeEntry.getValue();

      // check if the nodeEntryKey is mapComponent, $keys/$values
      Optional<MapComponentDesc> explicitMapComponent = MapComponentDesc.fromApiName(nodeEntryKey);
      if (explicitMapComponent.isPresent()) {
        // filter on map keys/values
        // i.e. {"$keys": {"$nin" : ["key1", "key2"], "$in": ["key3", "key4"]}}
        comparisonExpressions.addAll(
            filterMapColumnKeysOrValues(
                columnName,
                MapSetListFilterComponent.fromMapComponentDesc(explicitMapComponent.get()),
                nodeEntryValue));
      } else {
        // otherwise, filter on map entries
        // i.e. {"$in": [["key1", "value1"], ["key2", "value2"]]}

        // now we want to build a new JsonNode to represent single operationNode.
        var operationNode = OBJECT_MAPPER.createObjectNode().set(nodeEntryKey, nodeEntryValue);
        var filterOperation =
            singleFilterOperationForMapSetListColumn(
                operationNode, MapSetListFilterComponent.MAP_ENTRY, columnName);
        comparisonExpressions.add(
            new ComparisonExpression(
                columnName, List.of(filterOperation), null, MapSetListFilterComponent.MAP_ENTRY));
      }
    }
    return comparisonExpressions;
  }

  /**
   * Helper method to create ComparisonExpressions for filter keys/values to mapColumn.
   *
   * <p>Example 1 (Will result as two ComparisonExpressions): KeysOrValues E.G: $keys. filterValue
   * JsonNode E.G. {"$nin": ["key1", "key2"], "$in": ["key3", "key4"]}
   */
  private List<ComparisonExpression> filterMapColumnKeysOrValues(
      String columnName, MapSetListFilterComponent keysOrValues, JsonNode filterValue) {

    List<ComparisonExpression> result = new ArrayList<>();
    // $keys and $values must follow with a JsonNode object
    if (!filterValue.isObject()) {
      // "mapColumn": {"$keys": ["key1","key2"]} -> ["key1","key2"] is not object
      // "mapColumn": {"$values": 123} -> 123 is not object
      throw FilterException.Code.INVALID_MAP_SET_LIST_FILTER.get(
          errVars(
              schema,
              map -> {
                map.put(
                    "detailedReason",
                    "Invalid filter for map column '%s', please see the examples for the correct format"
                        .formatted(columnName));
              }));
    }
    // $keys": {"$nin": ["key1", "key2"], "$in": ["key3", "key4"]} can result with multiple
    // comparisonExpressions
    filterValue
        .fields()
        .forEachRemaining(
            entry -> {
              // build a new JsonNode to represent single operationNode.
              // i.e. build entry {"$nin": ["key1", "key2"]} to a JsonNode
              JsonNode operationNode =
                  OBJECT_MAPPER.createObjectNode().set(entry.getKey(), entry.getValue());
              var filterOperation =
                  singleFilterOperationForMapSetListColumn(operationNode, keysOrValues, columnName);
              result.add(
                  new ComparisonExpression(
                      columnName, List.of(filterOperation), null, keysOrValues));
            });
    return result;
  }

  /**
   * Validates that the provided `JsonNode` is an array, where each element is itself an array of
   * size 2. This is used to ensure proper tuple formatting for map entry filters.
   *
   * <p>Example of a valid `JsonNode`: <code>
   * {"$in": [["key1", "value1"], ["key2", "value2"]]}
   * </code>
   */
  private void checkMapTupleFormat(JsonNode tupleFormatEntryArray, String columnName) {

    boolean validTuple = true;
    // Ensure nodeEntryValue is a JSON array
    if (!tupleFormatEntryArray.isArray()) {
      // all good
      validTuple = false;
    }

    // Tuple map entries are represented as an array of arrays where each inner array has two
    // elements.
    if (validTuple) {
      for (JsonNode entry : tupleFormatEntryArray) {
        if (entry.getNodeType() != JsonNodeType.ARRAY || entry.size() != 2) {
          validTuple = false;
        }
      }
    }
    if (validTuple) {
      return;
    }

    throw FilterException.Code.INVALID_MAP_SET_LIST_FILTER.get(
        errVars(
            schema,
            map -> {
              map.put(
                  "detailedReason",
                  "Invalid usage for map entry filter in tuple format for map column '%s', ensure the node is an array of arrays, where each inner array has exactly two element to represent the key and value"
                      .formatted(columnName));
            }));
  }

  /**
   * To filter on a set/list column, we only need to check the operator.
   *
   * <p>allowed filter operators: <code>$in</code>, <code>$nin</code>, <code>$all</code>.
   *
   * <p>pathValue JsonNode E.G.
   *
   * <ul>
   *   <li><code>{"$nin": ["value3", "value4"], "$in": ["value1", "value2"]}</code>
   *   <li><code>{"$all": ["value1", "value2"]}</code>
   * </ul>
   */
  private List<ComparisonExpression> filterSetListColumn(
      String columnName, JsonNode pathValue, ApiTypeName setOrList) {

    final List<ComparisonExpression> result = new ArrayList<>();
    final Iterator<Map.Entry<String, JsonNode>> fields = pathValue.fields();
    // iterate through the filter fields.
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> expressionField = fields.next();

      var operator = expressionField.getKey();
      var operatorValue = expressionField.getValue();

      // build a new JsonNode to represent single operationNode.
      // i.e. build entry {$nin": ["value3", "value4"]} to a JsonNode
      JsonNode operationNode = OBJECT_MAPPER.createObjectNode().set(operator, operatorValue);
      var filterComponent =
          setOrList == ApiTypeName.LIST
              ? MapSetListFilterComponent.LIST_VALUE
              : MapSetListFilterComponent.SET_VALUE;
      FilterOperation<?> filterOperation =
          singleFilterOperationForMapSetListColumn(operationNode, filterComponent, columnName);

      result.add(
          new ComparisonExpression(columnName, List.of(filterOperation), null, filterComponent));
    }
    return result;
  }

  /**
   * Build single filter operation for map column. Notice, this method does not know the specific
   * column type, i.e. map/set/list. This logic isolation is made on purpose for better readability
   * and unit test. Also, operationNode must be a JsonNode with single entry.
   *
   * <p>E.G.
   *
   * <ul>
   *   <li><code>{"$in": [["key1","value1"], ["key2","value2"]]}</code>
   *   <li><code>{"$nin": ["value1","value2]}</code>
   *   <li><code>{"$all": ["key1","key2]}</code>
   *   <li><code>{"$in": ["listValue1", "listValue2"]}}</code> ...
   * </ul>
   */
  private FilterOperation<?> singleFilterOperationForMapSetListColumn(
      JsonNode operationNode, MapSetListFilterComponent mapSetListComponent, String columnName) {

    Map.Entry<String, JsonNode> singleEntry = operationNode.fields().next();
    // resolve the operator
    String operatorName = singleEntry.getKey();
    if (!SUPPORTED_MAP_SET_LIST_OPERATORS.contains(operatorName)) {
      throw FilterException.Code.INVALID_MAP_SET_LIST_FILTER.get(
          errVars(
              schema,
              map -> {
                map.put(
                    "detailedReason",
                    "Invalid filter operator '%s' for column '%s', allowed operators are %s"
                        .formatted(
                            operatorName,
                            columnName,
                            ERR_MESSAGE_SUPPORTED_MAP_SET_LIST_OPERATORS));
              }));
    }
    FilterOperator convertedOperator = FilterOperators.findComparisonOperator(operatorName);
    // the check above for SUPPORTED_MAP_SET_LIST_OPERATORS ensures that we know the operator is
    // so this is just a sanity check things are in sync
    Objects.requireNonNull(
        convertedOperator,
        "FilterOperators.findComparisonOperator() returned null for a supported filter operation, operatorName:%s, columnName:%s"
            .formatted(operatorName, columnName));

    // resolve the operation value
    JsonNode operationValueNode = singleEntry.getValue();

    // check tuple format if map component is MAP_ENTRY
    if (mapSetListComponent == MapSetListFilterComponent.MAP_ENTRY) {
      checkMapTupleFormat(operationValueNode, columnName);
    } else {
      // we are looking at a set, list, or the keyor values of a map so want an array
      if (!operationValueNode.isArray()) {
        throw FilterException.Code.INVALID_MAP_SET_LIST_FILTER.get(
            errVars(
                schema,
                map -> {
                  map.put(
                      "detailedReason",
                      "Filter operator '%s' for column '%s' must be followed by an array of values, see the examples for the correct format"
                          .formatted(operatorName, columnName));
                }));
      }
    }

    Object operationValue = jsonNodeValue(singleEntry.getValue());
    return ValueComparisonOperation.build(convertedOperator, operationValue, mapSetListComponent);
  }
}
