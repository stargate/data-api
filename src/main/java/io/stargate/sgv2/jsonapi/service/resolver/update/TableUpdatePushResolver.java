package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperatorModifier;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAppendToAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Resolver to resolve $push argument to List of ColumnAssignment. */
public class TableUpdatePushResolver implements TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#PUSH} operation.
   *
   * <p>Push operator can only be used for collection columns (list, set, map).
   *
   * <p>See {@link #resolvePushForListSet(TableSchemaObject, JsonNode)} for list/set columns. See
   * {@link #resolvePushForListSet(TableSchemaObject, JsonNode)} for map columns.
   *
   * @param table TableSchemaObject
   * @param arguments ObjectNode value for $push entry
   * @return list of columnAssignment for all the $push column updates
   */
  @Override
  public List<ColumnAssignment> resolve(TableSchemaObject table, ObjectNode arguments) {
    return arguments.properties().stream()
        .map(
            entry -> {
              var column = entry.getKey();
              var apiColumnDef = checkUpdateColumnExists(table, column);
              var inputValue = entry.getValue();

              // $push only works for map/set/list column
              checkUpdateOperatorSupportOnColumn(apiColumnDef, table, UpdateOperator.PUSH);

              JsonLiteral<?> shreddedValue = null;
              switch (apiColumnDef.type().typeName()) {
                case SET, LIST -> shreddedValue = resolvePushForListSet(table, inputValue);
                case MAP -> shreddedValue = resolvePushForMap(table, inputValue);
                default ->
                    throw new IllegalStateException("Unsupported column type for $push operation");
              }

              return new ColumnAssignment(
                  new ColumnAppendToAssignment(),
                  table.tableMetadata(),
                  CqlIdentifierUtil.cqlIdentifierFromUserInput(column),
                  shreddedValue);
            })
        .toList();
  }

  /**
   * Resolve $push operator value for list/set column.
   *
   * <p>Example(Push single element to the list/set):
   *
   * <ul>
   *   <li>list. <code>{"$push" : {"textList" : "textValue", "intList" : 111}}</code>
   *   <li>set. <code>{"$push" : {"textSet" : "textValue", "intSet" : 111}}</code>
   * </ul>
   *
   * <p>Example(Push multiple elements to the list/set):
   *
   * <ul>
   *   <li>list. <code>
   *       {"$push" : {"textList" : {"$each": ["textValue1", "textValue2"]}, "intList" : {"$each": [1,2]}}}
   *       </code>
   *   <li>set. <code>
   *       {"$push" : {"textSet" : {"$each": ["textValue1", "textValue2"]}, "intSet" : {"$each": [1,2]}}}
   *       </code>
   * </ul>
   *
   * TODO $position for list column
   *
   * @param table TableSchemaObject
   * @param inputValue jsonNode value for the $push column, E.G. if the operator is {"$push" :
   *     {"textList" : {"$each": ["textValue1", "textValue2"]}}, then inputValue is {"$each":
   *     ["textValue1", "textValue2"]}
   * @return JsonLiteral
   */
  private JsonLiteral<?> resolvePushForListSet(TableSchemaObject table, JsonNode inputValue) {
    JsonLiteral<?> shreddedValue;

    switch (inputValue.getNodeType()) {
      case ARRAY:
        // $push without $each, only work for adding single element
        throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
            errVars(
                table,
                map -> {
                  map.put("reason", "combine $push and $each for adding multiple elements");
                }));
      case OBJECT:
        // $push + $each for adding multiple elements
        ObjectNode objectNode = (ObjectNode) inputValue;
        if (objectNode.size() == 1
            && objectNode.get(UpdateOperatorModifier.EACH.apiName()) != null
            && objectNode.get(UpdateOperatorModifier.EACH.apiName()).getNodeType()
                == JsonNodeType.ARRAY) {
          shreddedValue =
              RowShredder.shredValue(objectNode.get(UpdateOperatorModifier.EACH.apiName()));
        } else {
          // invalid usage of $push + $each
          throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
              errVars(
                  table,
                  map -> {
                    map.put("reason", "invalid usage of $each");
                  }));
        }
        break;
      default:
        // $push with single element, normalize to List values.
        // this is helpful for further update assignment resolve.
        shreddedValue =
            new JsonLiteral<>(List.of(RowShredder.shredValue(inputValue)), JsonType.ARRAY);
        break;
    }
    return shreddedValue;
  }

  /**
   * Resolve $push operator value for map column.
   *
   * <p>Example(Push single element to the map):
   *
   * <ul>
   *   <li>map.(object format) <code>{"$push" : {"textToTextMap" : {"key1": "value1"}}}</code>
   *   <li>map.(tuple format) <code>{"$push" : {"textToTextMap" : ["key1", "value1"]}}</code>
   *   <li>map.(tuple format) <code>{"$push" : {"intToTextMap" : [1, "value1"]}}</code>
   * </ul>
   *
   * <p>Example(Push multiple elements to the map):
   *
   * <ul>
   *   <li>map. (object format)<code>
   *       {"$push" : { "textToTextMap" : {"$each": [{"key1": "value1"}, {"key2": "value2"}]}}
   *       </code>
   *   <li>map. (tuple format)<code>
   *       {"$push" : { "textToTextMap" : {"$each": [["key1","value1"], ["key2","value2"]]}}</code>
   *   <li>map. (tuple format)<code>
   *       {"$push" : { "intToTextMap" : {"$each": [[1,"value1"], [2,"value2"]]}}</code>
   * </ul>
   *
   * @param table TableSchemaObject
   * @param inputValue jsonNode value for the $push column, E.G. if the operator is {"$push" : {
   *     "textToTextMap" : {"$each": [{"key1": "value1"}, {"key2": "value2"}]}}, then inputValue is
   *     {"$each": [{"key1": "value1"}, {"key2": "value2"}]}
   * @return JsonLiteral
   */
  private JsonLiteral<?> resolvePushForMap(TableSchemaObject table, JsonNode inputValue) {
    JsonLiteral<?> shreddedValue = null;
    switch (inputValue.getNodeType()) {
      case ARRAY -> {
        // $push single entry to the map, entry as tuple format
        // E.G. {"$push": {"mapColumn": [5, "value5"]}}
        ArrayNode entryNodeTupleFormat = (ArrayNode) inputValue;
        shreddedValue =
            new JsonLiteral<>(
                resolveMapEntryFromTupleFormat(table, entryNodeTupleFormat), JsonType.SUB_DOC);
      }
      case OBJECT -> {
        var modifier$eachValue = inputValue.get(UpdateOperatorModifier.EACH.apiName());
        if (modifier$eachValue != null) {
          // With $each
          if (modifier$eachValue.getNodeType() == JsonNodeType.ARRAY) {
            return resolvePushForMapWithEach(table, (ArrayNode) modifier$eachValue);
          } else {
            // invalid usage of $push + $each
            throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
                errVars(
                    table,
                    map -> {
                      map.put("reason", "invalid usage of $each, $each value needs to be an array");
                    }));
          }
        } else {
          // $push single entry to the map, entry as map format
          // E.G. {"$push": {"mapColumn": {"key1" : "value1"}}}
          shreddedValue =
              new JsonLiteral<>(
                  resolveMapEntryFromObjectFormat(table, (ObjectNode) inputValue),
                  JsonType.SUB_DOC);
        }
      }
      default ->
          throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
              errVars(
                  table,
                  map -> {
                    map.put("reason", "use correct $push format to update target map column");
                  }));
    }
    return shreddedValue;
  }

  /** Resolve $push operator value for map column with $each modifier. */
  private JsonLiteral<?> resolvePushForMapWithEach(
      TableSchemaObject table, ArrayNode arrayNodeForMultipleEntries) {
    Map<JsonLiteral<?>, JsonLiteral<?>> pushForMultipleMapEntries = new HashMap<>();
    arrayNodeForMultipleEntries.forEach(
        entryNode -> {
          if (entryNode.getNodeType() == JsonNodeType.ARRAY) {
            // E.G. {"$push": {"mapColumn": {$each: [[1,"value1"],[2, "value2"]]}}}
            pushForMultipleMapEntries.putAll(
                resolveMapEntryFromTupleFormat(table, (ArrayNode) entryNode));
          } else if (entryNode.getNodeType() == JsonNodeType.OBJECT) {
            // E.G. {"$push": {"mapColumn": {$each: [{"key1":"value1"},[{"key2":"value2"}]}}}
            pushForMultipleMapEntries.putAll(
                resolveMapEntryFromObjectFormat(table, (ObjectNode) entryNode));
          } else {
            throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
                errVars(
                    table,
                    map -> {
                      map.put("reason", "please use correct map entry format");
                    }));
          }
        });
    return new JsonLiteral<>(pushForMultipleMapEntries, JsonType.SUB_DOC);
  }

  /** Helper method to resolve single map entry from tuple format. */
  private Map<JsonLiteral<?>, JsonLiteral<?>> resolveMapEntryFromTupleFormat(
      TableSchemaObject table, ArrayNode singleEntryNodeTupleFormat) {
    // the arrayNode must be for a single entry
    singleEntryNodeTupleFormat.forEach(
        entryNode -> {
          if (entryNode.isObject() || entryNode.isArray()) {
            throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
                errVars(
                    table,
                    map -> {
                      map.put("reason", "combine $push and $each for adding multiple elements");
                    }));
          }
        });

    // As the tuple format to indicate a map entry, array size must be 2
    if (singleEntryNodeTupleFormat.size() != 2) {
      throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
          errVars(
              table,
              map -> {
                map.put(
                    "reason",
                    "To use tuple format for indicating a map entry, provided array must be size of 2");
              }));
    }

    Map<JsonLiteral<?>, JsonLiteral<?>> entryMap = new HashMap<>();
    entryMap.put(
        RowShredder.shredValue(singleEntryNodeTupleFormat.get(0)),
        RowShredder.shredValue(singleEntryNodeTupleFormat.get(1)));
    return entryMap;
  }

  /** Helper method to resolve a map entry from map format. */
  private Map<JsonLiteral<?>, JsonLiteral<?>> resolveMapEntryFromObjectFormat(
      TableSchemaObject table, ObjectNode entryNodeMapFormat) {
    // size for the objectNode must be 1, since it is single map entry
    // It can't be a node with multiple entry, E.G. {"key1" : "value1", "key2" : "value2"}
    if (entryNodeMapFormat.size() != 1) {
      throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
          errVars(
              table,
              map -> {
                map.put("reason", "combine $push and $each for adding multiple elements");
              }));
    }
    Map<JsonLiteral<?>, JsonLiteral<?>> entryMap = new HashMap<>();
    Map.Entry<String, JsonNode> entry = entryNodeMapFormat.fields().next();
    entryMap.put(
        new JsonLiteral<>(entry.getKey(), JsonType.STRING),
        RowShredder.shredValue(entry.getValue()));
    return entryMap;
  }
}
