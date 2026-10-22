package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.UpdateClauseDeserializer;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.*;
import java.util.stream.Collectors;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;

/**
 * Value type read from incoming JSON and used for resolving into actual {@link UpdateOperation}s
 * with validation and additional context.
 */
@JsonDeserialize(using = UpdateClauseDeserializer.class)
@Schema(
    name = "UpdateClause",
    type = SchemaType.OBJECT,
    implementation = Object.class,
    additionalProperties = Schema.False.class,
    description =
        "A Data API update object mapping operators such as $set and $unset to objects of document paths and values. Operators may be combined on distinct paths. Supported operations depend on the target collection or table.",
    properties = {
      @SchemaProperty(
          name = UpdateOperator.Names.SET,
          type = SchemaType.OBJECT,
          description =
              "Sets paths to JSON values, including null, arrays, objects, and extended JSON."),
      @SchemaProperty(
          name = UpdateOperator.Names.SET_ON_INSERT,
          type = SchemaType.OBJECT,
          description = "Sets paths only when an upsert inserts a document."),
      @SchemaProperty(
          name = UpdateOperator.Names.UNSET,
          type = SchemaType.OBJECT,
          description = "Removes paths. The values supplied for them are ignored."),
      @SchemaProperty(
          name = UpdateOperator.Names.INC,
          type = SchemaType.OBJECT,
          description = "Maps paths to numeric increments, including fractions."),
      @SchemaProperty(
          name = UpdateOperator.Names.MUL,
          type = SchemaType.OBJECT,
          description = "Maps paths to numeric multipliers, including fractions."),
      @SchemaProperty(
          name = UpdateOperator.Names.MIN,
          type = SchemaType.OBJECT,
          description = "Updates paths when the supplied values are smaller."),
      @SchemaProperty(
          name = UpdateOperator.Names.MAX,
          type = SchemaType.OBJECT,
          description = "Updates paths when the supplied values are larger."),
      @SchemaProperty(
          name = UpdateOperator.Names.RENAME,
          type = SchemaType.OBJECT,
          description = "Maps existing paths to new path strings."),
      @SchemaProperty(
          name = UpdateOperator.Names.CURRENT_DATE,
          type = SchemaType.OBJECT,
          description = "Sets paths to the current date using true or {\"$type\": \"date\"}."),
      @SchemaProperty(
          name = UpdateOperator.Names.POP,
          type = SchemaType.OBJECT,
          description = "Removes the first (-1) or last (1) element of arrays."),
      @SchemaProperty(
          name = UpdateOperator.Names.PUSH,
          type = SchemaType.OBJECT,
          description =
              "Appends array values. Use $each for multiple values and optional $position."),
      @SchemaProperty(
          name = UpdateOperator.Names.ADD_TO_SET,
          type = SchemaType.OBJECT,
          description = "Adds unique array values. Use {\"$each\": [...]} for multiple values.")
    },
    examples = {
      """
        {
          "$set" : {"location": "New York"},
          "$unset" : {"obsolete": 1}
        }
        """,
      """
        {"$inc": {"score": 0.25}, "$mul": {"price": 1.5},
         "$min": {"lowest": 2.5}, "$max": {"highest": 9.5}}
        """,
      """
        {"$push": {"items": {"$each": [1, 2], "$position": 0}},
         "$addToSet": {"tags": {"$each": ["a", "b"]}},
         "$pop": {"history": -1}, "$rename": {"oldName": "newName"}}
        """,
      """
        {"$currentDate": {"updated": true},
         "$setOnInsert": {"created": {"$date": 123456}}, "$set": {"optional": null}}
        """,
      "{}"
    })
public record UpdateClause(
    @Schema(hidden = true) EnumMap<UpdateOperator, ObjectNode> updateOperationDefs) {
  /**
   * Method that will validate update operation definitions of the clause and construct an ordered
   * set of {@link UpdateOperation}s.
   *
   * @return List of update operations to execute
   */
  public List<UpdateOperation> buildOperations() {
    // First, resolve operations individually; this will handle basic validation
    final var operationMap = new EnumMap<UpdateOperator, UpdateOperation>(UpdateOperator.class);
    updateOperationDefs
        .entrySet()
        .forEach(e -> operationMap.put(e.getKey(), e.getKey().resolveOperation(e.getValue())));

    // Then handle cross-operation validation: first, exact path conflicts:
    final var actionMap = new TreeMap<PathMatchLocator, UpdateOperator>();
    operationMap
        .entrySet()
        .forEach(
            e -> {
              final UpdateOperator type = e.getKey();
              List<ActionWithLocator> actions = e.getValue().actions();
              for (ActionWithLocator action : actions) {
                UpdateOperator prevType = actionMap.put(action.locator(), type);
                if (prevType != null) {
                  throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PARAM.get(
                      Map.of(
                          "errorMessage",
                          "update operators '%s' and '%s' must not refer to same path: '%s'"
                              .formatted(prevType.apiName(), type.apiName(), action.locator())));
                }
              }
            });

    // Second: check for sub-paths -- efficiently done by using alphabetic-sorted paths
    // and comparing adjacent ones; for each pair see if later one is longer one and
    // starts with dot.
    // For example: "path.x" vs "path.x.some.more" we notice latter has suffix ".some.more"
    // and is thereby conflicting.
    if (!actionMap.isEmpty()) {
      var it = actionMap.entrySet().iterator();
      var prev = it.next();

      while (it.hasNext()) {
        var curr = it.next();
        PathMatchLocator prevLoc = prev.getKey();
        PathMatchLocator currLoc = curr.getKey();
        // So, if parent/child, parent always first so this check is enough
        if (currLoc.isSubPathOf(prevLoc)) {
          throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PARAM.get(
              Map.of(
                  "errorMessage",
                  "Update operator path conflict due to overlap: '%s' (%s) vs '%s' (%s)"
                      .formatted(
                          prevLoc, prev.getValue().apiName(), currLoc, curr.getValue().apiName())));
        }
      }
    }

    return new ArrayList<>(operationMap.values());
  }

  private Set<String> getPaths(UpdateOperation<?> updateOp) {
    return updateOp.actions().stream().map(act -> act.path()).collect(Collectors.toSet());
  }
}
