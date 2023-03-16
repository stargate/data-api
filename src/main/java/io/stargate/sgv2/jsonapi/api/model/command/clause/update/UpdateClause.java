package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.UpdateClauseDeserializer;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Value type read from incoming JSON and used for resolving into actual {@link UpdateOperation}s
 * with validation and additional context.
 */
@JsonDeserialize(using = UpdateClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Object.class,
    example =
        """
        {
          "$set" : {"location": "New York"},
          "$unset" : {"new_data": 1}
        }
        """)
public record UpdateClause(EnumMap<UpdateOperator, ObjectNode> updateOperationDefs) {
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
                  throw new JsonApiException(
                      ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
                      "Update operators '%s' and '%s' must not refer to same path: '%s'"
                          .formatted(prevType.operator(), type.operator(), action.locator()));
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
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
              "Update operator path conflict due to overlap: '%s' (%s) vs '%s' (%s)"
                  .formatted(
                      prevLoc, prev.getValue().operator(), currLoc, curr.getValue().operator()));
        }
      }
    }
    actionMap.entrySet().forEach(e -> {});

    return new ArrayList<>(operationMap.values());
  }

  private Set<String> getPaths(UpdateOperation<?> updateOp) {
    return updateOp.actions().stream().map(act -> act.path()).collect(Collectors.toSet());
  }
}
