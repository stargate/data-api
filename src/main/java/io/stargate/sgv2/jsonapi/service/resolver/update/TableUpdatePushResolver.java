package io.stargate.sgv2.jsonapi.service.resolver.update;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errVars;
import static io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperatorModifier;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAppendToAssignment;
import io.stargate.sgv2.jsonapi.service.operation.query.ColumnAssignment;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue;
import java.util.List;

/** Resolver to resolve $push argument to List of ColumnAssignment. */
public class TableUpdatePushResolver extends TableUpdateOperatorResolver {

  /**
   * Resolve the {@link UpdateOperator#PUSH} operation.
   *
   * <p>Push operator can only be used for collection columns (list, set, map).
   *
   * <p>See {@link #resolvePushForListSet(TableSchemaObject, JsonNode)} for list/set columns. See
   * {@link #resolvePushForListSet(TableSchemaObject, JsonNode)} for map columns.
   *
   * @param tableSchemaObject TableSchemaObject
   * @param arguments ObjectNode value for $push entry, if the command has <code>
   *     {"$push" : {"textList" : "textValue", "intList" : 111}}</code> this is <code>
   *     {"textList" : "textValue", "intList" : 111}</code>
   * @return list of columnAssignment for all the $push column updates
   */
  @Override
  public List<ColumnAssignment> resolve(
      TableSchemaObject tableSchemaObject,
      CqlNamedValue.ErrorStrategy<UpdateException> errorStrategy,
      ObjectNode arguments) {

    // normalise the RHS arg to $push so it looks like an insert document for multiple columns
    // where the value is always an array
    // the way we parse the $push value is different for list/set and map
    var normalisedPushDoc = JsonNodeFactory.instance.objectNode();
    arguments
        .properties()
        .forEach(
            pushOp -> {
              // the name - value pair from the $push
              var apiColumn =
                  tableSchemaObject
                      .apiTableDef()
                      .allColumns()
                      .get(cqlIdentifierFromUserInput(pushOp.getKey()));

              // if we cannot find the column in the table, OR it is not the type that is OK
              // we will detect that during the binding and preparing the values for the query
              // so just copy the raw push value into the normalised doc.
              normalisedPushDoc.set(
                  pushOp.getKey(),
                  switch (apiColumn) {
                    case ApiColumnDef col when (col.type().typeName() == SET
                            || col.type().typeName() == LIST) ->
                        normaliseListSet(tableSchemaObject, pushOp.getValue());
                    case ApiColumnDef col when col.type().typeName() == MAP ->
                        normaliseMap(tableSchemaObject, pushOp.getValue());
                    default -> normalisedPushDoc.set(pushOp.getKey(), pushOp.getValue());
                  });
            });

    // we now have a normalised doc that looks like an insert, but will always use the array of
    // tuples
    // format for the map values
    return createColumnAssignments(
        tableSchemaObject,
        arguments,
        errorStrategy,
        UpdateOperator.PUSH,
        ColumnAppendToAssignment::new);

    //    return arguments.properties().stream()
    //        .map(
    //            entry -> {
    //
    //              JsonLiteral<?> shreddedValue = null;
    //              switch (apiColumnDef.type().typeName()) {
    //                case SET, LIST -> shreddedValue = resolvePushForListSet(tableSchemaObject,
    // inputValue);
    //                case MAP -> shreddedValue = resolvePushForMap(tableSchemaObject, inputValue);
    //                default ->
    //                    throw new IllegalStateException("Unsupported column type for $push
    // operation");
    //              }
    //
    //              return new ColumnAssignment(
    //                  new ColumnAppendToAssignment(),
    //                  tableSchemaObject.tableMetadata(),
    //                  cqlIdentifierFromUserInput(column),
    //                  shreddedValue);
    //            })
    //        .toList();
    //
    //
    //    // $push only works for map/set/list column
    //    checkUpdateOperatorSupported(tableSchemaObject, allColumns, UpdateOperator.PUSH);
    //  }
    //        return null;
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
   * @param tableSchemaObject TableSchemaObject
   * @param opRHS The Right Hand Side operand of, e.g. if the op is <code>
   *     {"$push" : {"textList" : "textValue", "intList" : 111}}</code> the RHS is <code>
   *     {"textList" : "textValue", "intList" : 111}</code>
   * @return
   */
  private ArrayNode normaliseListSet(TableSchemaObject tableSchemaObject, JsonNode opRHS) {

    // we normalise whatever the $push format provided by the user into q JSON object of
    // column name to an array
    // {"textList" : ["textValue"], "intSet" : [1,2,3], "fieldName : ["value1", "value2"]}
    return switch (opRHS) {
      case ArrayNode ignored ->
          // $push without $each, only work for adding single element but we have an array.
          throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
              errVars(
                  tableSchemaObject,
                  map -> {
                    map.put("reason", "combine $push and $each for adding multiple elements");
                  }));

      case ObjectNode objectNode ->
          // $push + $each for adding multiple elements, we should have the obj with $each here
          // {"$push" : {"textList" : {"$each": ["textValue1", "textValue2"]}
          getValidEachRHS(tableSchemaObject, objectNode, true);

      default ->
          // $push with single element, .e.g
          // {"$push" : {"textList" : "textValue",
          // we want to create  {"textList" : ["textValue"]}
          JsonNodeFactory.instance.arrayNode(1).add(opRHS);
    };
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
   * @param tableSchemaObject TableSchemaObject
   * @param inputValue jsonNode value for the $push column, E.G. if the operator is {"$push" : {
   *     "textToTextMap" : {"$each": [{"key1": "value1"}, {"key2": "value2"}]}}, then inputValue is
   *     {"$each": [{"key1": "value1"}, {"key2": "value2"}]}
   * @return JsonLiteral
   */
  private ArrayNode normaliseMap(TableSchemaObject tableSchemaObject, JsonNode opRHS) {

    // we normalise whatever the $push format provided by the user into a JSON object of
    // column name to an array of tuples for the map entries
    // {"mapColumn" : [ [key, value], [key, value] ]}
    return switch (opRHS) {
      case ArrayNode arrayNode -> {
        // $push single entry to the map, entry as tuple format
        // E.G. {"$push": {"mapColumn": [5, "value5"]}}
        // normalised value mapColumn is [[5, "value5"]]
        checkMapTupleFormat(tableSchemaObject, arrayNode);
        yield JsonNodeFactory.instance.arrayNode(1).add(arrayNode);
      }

      case ObjectNode objectNode -> {
        // this must be using $each, could be either of
        // {"$push" : { "textToTextMap" : {"$each": [{"key1": "value1"},
        // {"$push" : { "intToTextMap" : {"$each": [[1,"value1"],
        // OR the non each push a map
        // {"$push" : {"textToTextMap" : {"key1": "value1"}}}

        var eachOpRHS = getValidEachRHS(tableSchemaObject, objectNode, false);
        if (eachOpRHS == null) {
          // we have the non each push to a map
          // {"$push" : {"textToTextMap" : {"key1": "value1"}}}
          // we have a mapEntry in objectNode, i.e. {"key1": "value1"}
          yield JsonNodeFactory.instance
              .arrayNode(1)
              .add(mapEntryToTuple(tableSchemaObject, objectNode));
        }

        // we have the $each push to a map
        // could be array of map entries  or array of tuple, or mixed of both
        // {"$push" : { "textToTextMap" : {"$each": [{"key1": "value1"},
        // {"$push" : { "intToTextMap" : {"$each": [[1,"value1"],
        yield normaliseMapEachArray(tableSchemaObject, eachOpRHS);
      }

      default -> // TODO: better error
          throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
              errVars(
                  tableSchemaObject,
                  map -> {
                    map.put("reason", "use correct $push format to update target map column");
                  }));
    };
  }
  ;

  //    switch (inputValue.getNodeType()) {
  //      case ARRAY -> {
  //        // $push single entry to the map, entry as tuple format
  //        // E.G. {"$push": {"mapColumn": [5, "value5"]}}
  //        ArrayNode entryNodeTupleFormat = (ArrayNode) inputValue;
  //        shreddedValue =
  //            new JsonLiteral<>(
  //                resolveMapEntryFromTupleFormat(tableSchemaObject, entryNodeTupleFormat),
  // JsonType.SUB_DOC);
  //      }
  //      case OBJECT -> {
  //        var modifier$eachValue = inputValue.get(UpdateOperatorModifier.EACH.apiName());
  //        if (modifier$eachValue != null) {
  //          // With $each
  //          if (modifier$eachValue.getNodeType() == JsonNodeType.ARRAY) {
  //            return resolvePushForMapWithEach(tableSchemaObject, (ArrayNode) modifier$eachValue);
  //          } else {
  //            // invalid usage of $push + $each
  //            throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
  //                errVars(
  //                    tableSchemaObject,
  //                    map -> {
  //                      map.put("reason", "invalid usage of $each, $each value needs to be an
  // array");
  //                    }));
  //          }
  //        } else {
  //          // $push single entry to the map, entry as map format
  //          // E.G. {"$push": {"mapColumn": {"key1" : "value1"}}}
  //          shreddedValue =
  //              new JsonLiteral<>(
  //                  resolveMapEntryFromObjectFormat(tableSchemaObject, (ObjectNode) inputValue),
  //                  JsonType.SUB_DOC);
  //        }
  //      }
  //      default ->
  //          throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
  //              errVars(
  //                  tableSchemaObject,
  //                  map -> {
  //                    map.put("reason", "use correct $push format to update target map column");
  //                  }));
  //    }
  //    return shreddedValue;
  //  }

  private ArrayNode getValidEachRHS(
      TableSchemaObject tableSchemaObject, ObjectNode objectNode, boolean throwIfMissing) {
    // {"$push" : {"textList" : {"$each": ["textValue1", "textValue2"]}
    var eachOpRHS = objectNode.get(UpdateOperatorModifier.EACH.apiName());

    if (eachOpRHS == null && !throwIfMissing) {
      return null;
    }

    if (objectNode.size() != 1 || !(eachOpRHS instanceof ArrayNode arrayNode)) {
      throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put("reason", "invalid usage of $each, $each value needs to be an array");
              }));
    }
    return arrayNode;
  }

  /**
   * The map $each array can be an array of tuples or an array of entries:
   *
   * <pre>
   * // Array of entries
   * {"$push" : { "textToTextMap" : {"$each": [{"key1": "value1"}, {"key2": "value2"}]}}
   *
   * // array of tuples, using string keys
   * {"$push" : { "textToTextMap" : {"$each": [["key1","value1"], ["key2","value2"]]}}</code>
   *
   * // array of tuples, using non string keys
   * {"$push" : { "intToTextMap" : {"$each": [[1,"value1"], [2,"value2"]]}}</code>
   *
   * // or an array of mixed
   * {"$push" : { "textToTextMap" : {"$each": [{"key1": "value1"}, ["key2","value2"]]}}
   * </pre>
   *
   * @param tableSchemaObject
   * @param eachOpRHS The value of the <code>$each</code> in the above.
   * @return The normalised array of tuple pairs, e.g. <code>[[key1, value1], [key2, value2]]</code>
   */
  private ArrayNode normaliseMapEachArray(
      TableSchemaObject tableSchemaObject, ArrayNode eachOpRHS) {

    var normalisedEachArray = JsonNodeFactory.instance.arrayNode();

    eachOpRHS.forEach(
        entryNode -> {
          switch (entryNode) {
            case ArrayNode arrayNode -> {
              // is already a tuple
              checkMapTupleFormat(tableSchemaObject, arrayNode);
              normalisedEachArray.add(arrayNode);
            }
            case ObjectNode objectNode -> {
              normalisedEachArray.add(mapEntryToTuple(tableSchemaObject, objectNode));
            }
            default -> {
              throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
                  errVars(
                      tableSchemaObject,
                      map -> {
                        map.put("reason", "please use correct map entry format");
                      }));
            }
          }
          ;
        });

    if (eachOpRHS.size() != normalisedEachArray.size()) {
      throw new IllegalStateException(
          "Normalised array of map $each entries should be the same size as the original");
    }
    return normalisedEachArray;
  }

  private void checkMapTupleFormat(TableSchemaObject tableSchemaObject, ArrayNode singleMapTuple) {

    // the values in the tuple must be atomic , not object or array
    singleMapTuple.forEach(
        entryNode -> {
          if (entryNode.isObject() || entryNode.isArray()) {
            throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
                errVars(
                    tableSchemaObject,
                    map -> {
                      map.put("reason", "combine $push and $each for adding multiple elements");
                    }));
          }
        });

    // As the tuple format to indicate a map entry, array size must be 2
    if (singleMapTuple.size() != 2) {
      throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "reason",
                    "To use tuple format for indicating a map entry, provided array must be size of 2");
              }));
    }
  }

  private void checkMapEntryFormat(TableSchemaObject table, ObjectNode entryNodeMapFormat) {

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
  }

  private ArrayNode mapEntryToTuple(TableSchemaObject table, ObjectNode mapEntry) {
    checkMapEntryFormat(table, mapEntry);
    // there will the format check makes sure we only have 1
    var keyValue = mapEntry.fields().next();
    return JsonNodeFactory.instance.arrayNode(2).add(keyValue.getKey()).add(keyValue.getValue());
  }

  //  /** Helper method to resolve single map entry from tuple format. */
  //  private Map<JsonLiteral<?>, JsonLiteral<?>> resolveMapEntryFromTupleFormat(
  //      TableSchemaObject table, ArrayNode singleEntryNodeTupleFormat) {
  //    // the arrayNode must be for a single entry
  //    singleEntryNodeTupleFormat.forEach(
  //        entryNode -> {
  //          if (entryNode.isObject() || entryNode.isArray()) {
  //            throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
  //                errVars(
  //                    table,
  //                    map -> {
  //                      map.put("reason", "combine $push and $each for adding multiple elements");
  //                    }));
  //          }
  //        });
  //
  //    // As the tuple format to indicate a map entry, array size must be 2
  //    if (singleEntryNodeTupleFormat.size() != 2) {
  //      throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
  //          errVars(
  //              table,
  //              map -> {
  //                map.put(
  //                    "reason",
  //                    "To use tuple format for indicating a map entry, provided array must be size
  // of 2");
  //              }));
  //    }
  //
  //    Map<JsonLiteral<?>, JsonLiteral<?>> entryMap = new HashMap<>();
  //    entryMap.put(
  //        RowShredder.shredValue(singleEntryNodeTupleFormat.get(0)),
  //        RowShredder.shredValue(singleEntryNodeTupleFormat.get(1)));
  //    return entryMap;
  //  }

  /** Helper method to resolve a map entry from map format. */
  //  private Map<JsonLiteral<?>, JsonLiteral<?>> resolveMapEntryFromObjectFormat(
  //      TableSchemaObject table, ObjectNode entryNodeMapFormat) {
  //    // size for the objectNode must be 1, since it is single map entry
  //    // It can't be a node with multiple entry, E.G. {"key1" : "value1", "key2" : "value2"}
  //    if (entryNodeMapFormat.size() != 1) {
  //      throw UpdateException.Code.INVALID_PUSH_OPERATOR_USAGE.get(
  //          errVars(
  //              table,
  //              map -> {
  //                map.put("reason", "combine $push and $each for adding multiple elements");
  //              }));
  //    }
  //    Map<JsonLiteral<?>, JsonLiteral<?>> entryMap = new HashMap<>();
  //    Map.Entry<String, JsonNode> entry = entryNodeMapFormat.fields().next();
  //    entryMap.put(
  //        new JsonLiteral<>(entry.getKey(), JsonType.STRING),
  //        RowShredder.shredValue(entry.getValue()));
  //    return entryMap;
  //  }
}
