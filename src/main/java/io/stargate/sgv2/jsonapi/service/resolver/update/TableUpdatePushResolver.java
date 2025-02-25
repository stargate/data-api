package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonLiteral;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowShredder;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableUpdatePushResolver implements TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#PUSH} operation.
   *
   * <p>Push operator can only be used for collection columns (list, set, map).
   *
   * <p>Example(Push single element to the collection):
   *
   * <ul>
   *   <li>list. <code>{"$push" : {"textList" : "textValue", "intList" : 111}}</code>
   *   <li>set. <code>{"$push" : {"textSet" : "textValue", "intSet" : 111}}</code>
   *   <li>map.(object format) <code>{"$push" : {"textToTextMap" : {"key1": "value1"}}}</code>
   *   <li>map.(tuple format) <code>{"$push" : {"textToTextMap" : ["key1", "value1"]}}</code>
   *   <li>map.(tuple format) <code>{"$push" : {"intToTextMap" : [1, "value1"]}}</code>
   * </ul>
   *
   * <p>Example(Push multiple elements to the collection):
   *
   * <ul>
   *   <li>list. <code>
   *       {"$push" : {"textList" : {"$each": ["textValue1", "textValue2"]}, "intList" : {"$each": [1,2]}}}
   *       </code>
   *   <li>set. <code>
   *       {"$push" : {"textSet" : {"$each": ["textValue1", "textValue2"]}, "intSet" : {"$each": [1,2]}}}
   *       </code>
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
   * @param arguments ObjectNode value for $push entry
   * @return list of columnAssignment for $push ????
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
              if (apiColumnDef.type().isPrimitive()
                  || apiColumnDef.type().typeName() == ApiTypeName.VECTOR) {
                throw UpdateException.Code.INVALID_USAGE_FOR_COLLECTION_ONLY_UPDATE_OPERATORS.get(
                    errVars(
                        table,
                        map -> {
                          map.put("operator", "$push");
                          map.put("targetColumn", errFmt(apiColumnDef.name()));
                        }));
              }

              JsonLiteral<?> shreddedValue = null;
              // resolve $push value for set/list column
              if (apiColumnDef.type().typeName() == ApiTypeName.SET
                  || apiColumnDef.type().typeName() == ApiTypeName.LIST) {
                shreddedValue = resolvePushForListSet(table, inputValue);
              }

              // resolve $push value for map column
              if (apiColumnDef.type().typeName() == ApiTypeName.MAP) {
                shreddedValue = resolvePushForMap(table, inputValue);
              }

              return new ColumnAssignment(
                  UpdateOperator.PUSH,
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
   * *TODO *
   *
   * <p>For list column, append is by default, use $position -1 for
   *
   * @param table TableSchemaObject
   * @param inputValue jsonNode value for the $push column
   * @return JsonLiteral
   */
  private JsonLiteral<?> resolvePushForListSet(TableSchemaObject table, JsonNode inputValue) {
    JsonLiteral<?> shreddedValue;

    if (inputValue.getNodeType() == JsonNodeType.ARRAY) {
      // $push without $each, only work for adding single element
      throw UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR.get(
          errVars(
              table,
              map -> {
                map.put("reason", "combine $push and $each for adding multiple elements");
              }));
    } else if (inputValue.getNodeType() == JsonNodeType.OBJECT) {
      // $push + $each for adding multiple elements
      ObjectNode objectNode = (ObjectNode) inputValue;
      if (objectNode.size() == 1 // TODO position
          && objectNode.get("$each") != null
          && objectNode.get("$each").getNodeType() == JsonNodeType.ARRAY) {
        shreddedValue = RowShredder.shredValue(objectNode.get("$each"));
      } else {
        // invalid usage of $push + $each
        throw UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR.get(
            errVars(
                table,
                map -> {
                  map.put("reason", "invalid usage of $each");
                }));
      }
    } else {
      // $push with single element
      shreddedValue =
          new JsonLiteral<>(List.of(RowShredder.shredValue(inputValue)), JsonType.ARRAY);
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
   * @param inputValue jsonNode value for the $push column
   * @return JsonLiteral
   */
  private JsonLiteral<?> resolvePushForMap(TableSchemaObject table, JsonNode inputValue) {
    JsonLiteral<?> shreddedValue = null;
    if (inputValue.getNodeType() == JsonNodeType.ARRAY) {
      // $push single entry to the map, entry as tuple format
      // E.G. {"$push": {"mapColumn": [5, "value5"]}}
      ArrayNode entryNodeTupleFormat = (ArrayNode) inputValue;
      shreddedValue =
          new JsonLiteral<>(
              resolveMapEntryFromTupleFormat(table, entryNodeTupleFormat), JsonType.SUB_DOC);

    } else if (inputValue.getNodeType() == JsonNodeType.OBJECT) {
      var modifier$eachValue = inputValue.get("$each");
      if (modifier$eachValue != null) {
        // With $each
        if (modifier$eachValue.getNodeType() == JsonNodeType.ARRAY) {
          ArrayNode arrayNodeForMultipleEntries = (ArrayNode) modifier$eachValue;
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
                  throw UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR.get(
                      errVars(
                          table,
                          map -> {
                            map.put("reason", "please use correct map entry format");
                          }));
                }
              });
          return new JsonLiteral<>(pushForMultipleMapEntries, JsonType.SUB_DOC);
        } else {
          // invalid usage of $push + $each
          throw UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR.get(
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
                resolveMapEntryFromObjectFormat(table, (ObjectNode) inputValue), JsonType.SUB_DOC);
      }
    } else {
      throw UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR.get(
          errVars(
              table,
              map -> {
                map.put("reason", "use correct $push format to update target map column");
              }));
    }
    return shreddedValue;
  }

  /** Helper method to resolve single map entry from tuple format. */
  private Map<JsonLiteral<?>, JsonLiteral<?>> resolveMapEntryFromTupleFormat(
      TableSchemaObject table, ArrayNode singleEntryNodeTupleFormat) {
    // the arrayNode must be for a single entry
    singleEntryNodeTupleFormat.forEach(
        entryNode -> {
          if (entryNode.isObject() || entryNode.isArray()) {
            throw UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR.get(
                errVars(
                    table,
                    map -> {
                      map.put("reason", "combine $push and $each for adding multiple elements");
                    }));
          }
        });

    // As the tuple format to indicate a map entry, array size must be 2
    if (singleEntryNodeTupleFormat.size() != 2) {
      throw UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR.get(
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
      throw UpdateException.Code.INVALID_USAGE_OF_PUSH_OPERATOR.get(
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
