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
        "The Update Clause to use when updating Documents in a Collection or Rows in a Table.",
    properties = {
      @SchemaProperty(
          name = UpdateOperator.Names.SET,
          type = SchemaType.OBJECT,
          description = "Sets fields to the provided JSON value."),
      @SchemaProperty(
          name = UpdateOperator.Names.SET_ON_INSERT,
          type = SchemaType.OBJECT,
          description =
              "Sets fields to the provided JSON value, only when an upsert inserts the document."),
      @SchemaProperty(
          name = UpdateOperator.Names.UNSET,
          type = SchemaType.OBJECT,
          description = "Removes fields from the Document, or sets Table columns to null."),
      @SchemaProperty(
          name = UpdateOperator.Names.INC,
          type = SchemaType.OBJECT,
          description =
              "Updates numeric fields by incrementing them by the provided value (use negative values to decrement)."),
      @SchemaProperty(
          name = UpdateOperator.Names.MUL,
          type = SchemaType.OBJECT,
          description = "Updates numeric fields by multiplying them by the provided value."),
      @SchemaProperty(
          name = UpdateOperator.Names.MIN,
          type = SchemaType.OBJECT,
          description =
              "Updates fields to the smaller of the current value or the provided value."),
      @SchemaProperty(
          name = UpdateOperator.Names.MAX,
          type = SchemaType.OBJECT,
          description = "Updates fields to the larger of the current value or the provided value."),
      @SchemaProperty(
          name = UpdateOperator.Names.RENAME,
          type = SchemaType.OBJECT,
          description = "Renames fields to the provided names."),
      @SchemaProperty(
          name = UpdateOperator.Names.CURRENT_DATE,
          type = SchemaType.OBJECT,
          description =
              "Sets fields to the current date. The value must be true or {\"$type\": \"date\"}."),
      @SchemaProperty(
          name = UpdateOperator.Names.POP,
          type = SchemaType.OBJECT,
          description =
              "Updates array fields to remove the first (-1) or the last (1) element of the array."),
      @SchemaProperty(
          name = UpdateOperator.Names.PUSH,
          type = SchemaType.OBJECT,
          description =
              "Updates array fields in a Document, or list, set, and map columns in a Table, by appending values. Use $each for multiple values. Only collections accept $position."),
      @SchemaProperty(
          name = UpdateOperator.Names.ADD_TO_SET,
          type = SchemaType.OBJECT,
          description =
              "Updates array fields in a Document by appending values that are not already present. Use $each for multiple values."),
      @SchemaProperty(
          name = UpdateOperator.Names.PULL_ALL,
          type = SchemaType.OBJECT,
          description =
              "Updates list and set columns in a Table by removing the provided values, and map columns by removing the provided keys.")
    },
    examples = {
      """
        {
          "$set": {"location": "New York"},
          "$unset": {"obsolete": 1}
        }
        """,
      """
        {
          "$inc": {"score": 0.25},
          "$mul": {"price": 1.5},
          "$min": {"lowest": 2.5},
          "$max": {"highest": 9.5}
        }
        """,
      """
        {
          "$push": {
            "items": {"$each": [1, 2], "$position": 0}
          },
          "$addToSet": {
            "tags": {"$each": ["a", "b"]}
          },
          "$pop": {"history": -1},
          "$rename": {"oldName": "newName"}
        }
        """,
      """
        {
          "$currentDate": {"updated": true},
          "$setOnInsert": {"created": {"$date": 123456}},
          "$set": {"optional": null}
        }
        """,
      """
        {
          "$pullAll": {"tags": ["a", "b"]}
        }
        """,
      "{}"
    })
// Hide parsed operations from schemas without changing Jackson serialization.
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
