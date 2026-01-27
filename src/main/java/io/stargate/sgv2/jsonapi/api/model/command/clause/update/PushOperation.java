package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import io.stargate.sgv2.jsonapi.util.PathMatch;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@code $push} update operation used to append values in array fields of
 * documents.
 */
public class PushOperation extends UpdateOperation<PushOperation.Action> {
  private PushOperation(List<Action> actions) {
    super(actions);
  }

  public static PushOperation construct(ObjectNode args) {
    List<Action> updates = new ArrayList<>();
    for (Map.Entry<String, JsonNode> entry : args.properties()) {
      // First verify update operation allowed for path (not for _id, $lexical/$vector/$vectorize)
      final String name = validateUpdatePath(UpdateOperator.PUSH, entry.getKey());
      // At main level we must have field name (no modifiers)
      if (looksLikeModifier(name)) {
        throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PARAM.get(
            Map.of(
                "errorMessage",
                "$push requires field names at main level, found modifier: %s".formatted(name)));
      }
      // But within field value modifiers are allowed: if there's one, all must be modifiers
      JsonNode value = entry.getValue();
      Action action;
      if (value.isObject() && hasModifier((ObjectNode) value)) {
        action = buildActionWithModifiers(name, (ObjectNode) value);
      } else {
        action = new Action(PathMatchLocator.forPath(name), entry.getValue(), false, null);
      }
      updates.add(action);
    }
    return new PushOperation(updates);
  }

  private static Action buildActionWithModifiers(String propName, ObjectNode actionDef) {
    // We know there is at least one modifier; and if so, all must be modifiers.
    // We support basic "$each" with optional "$position":
    JsonNode eachArg = null;
    Integer position = null;

    for (Map.Entry<String, JsonNode> entry : actionDef.properties()) {
      final String modifier = entry.getKey();
      final JsonNode arg = entry.getValue();

      switch (modifier) {
        case "$each":
          eachArg = arg;
          if (!eachArg.isArray()) {
            throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PARAM.get(
                Map.of(
                    "errorMessage",
                    "$push modifier $each requires Array argument, found: %s"
                        .formatted(JsonUtil.nodeTypeAsString(eachArg))));
          }
          break;
        case "$position":
          // Mongo requires number
          if (!arg.isNumber()) {
            throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PARAM.get(
                Map.of(
                    "errorMessage",
                    "$push modifier $position requires (integral) Number argument, found: %s"
                        .formatted(JsonUtil.nodeTypeAsString(arg))));
          }
          // but floating-point won't do either:
          if (!arg.isIntegralNumber()) {
            throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PARAM.get(
                Map.of(
                    "errorMessage",
                    "$push modifier $position requires Integer Number argument, instead got: %s"
                        .formatted(arg.asText())));
          }
          position = arg.intValue();
          break;

        default:
          throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.get(
              Map.of(
                  "errorMessage",
                  "$push only supports $each and $position currently; trying to use '%s'"
                      .formatted(modifier)));
      }
    }
    // For now should not be possible to occur but once we add other modifiers could:
    if (eachArg == null) {
      throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_PARAM.get(
          Map.of(
              "errorMessage",
              "$push modifiers can only be used with $each modifier; none included"));
    }

    return new Action(PathMatchLocator.forPath(propName), eachArg, true, position);
  }

  private static boolean hasModifier(ObjectNode node) {
    // Sigh. Wish things were not returned as iterators by JsonNode...
    Iterator<String> it = node.fieldNames();
    while (it.hasNext()) {
      if (looksLikeModifier(it.next())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public UpdateOperationResult updateDocument(ObjectNode doc) {
    for (Action action : actions) {
      final JsonNode toAdd = action.value;

      PathMatch target = action.locator().findOrCreate(doc);
      JsonNode node = target.valueNode();

      ArrayNode array;

      if (node == null) { // No such property? Add new array
        array = doc.arrayNode();
        target.replaceValue(array);
      } else if (node.isArray()) { // Already array? Append
        array = (ArrayNode) node;
      } else { // Something else? fail
        throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION_TARGET.get(
            Map.of(
                "errorMessage",
                "$push requires target to be Array; value at '%s' of type %s"
                    .formatted(target.fullPath(), JsonUtil.nodeTypeAsString(node))));
      }
      // Regular add or $each?
      if (action.each) {
        // $position?
        if (action.position() != null) {
          int ix = (int) action.position();
          // Negative index is offset from the end, -1 being "before the last"
          if (ix < 0) {
            ix = Math.max(0, ix + array.size());
          } else {
            ix = Math.min(ix, array.size());
          }
          // ArrayNode.insert() can handle offsets [0..len]
          for (JsonNode element : toAdd) {
            array.insert(ix++, element);
          }
        } else {
          for (JsonNode element : toAdd) {
            array.add(element);
          }
        }
      } else {
        array.add(toAdd);
      }
    }

    // Every valid update operation modifies document so need just one:
    return new UpdateOperationResult(!actions.isEmpty(), List.of());
  }

  // Just needed for tests
  @Override
  public boolean equals(Object o) {
    return (o instanceof PushOperation)
        && Objects.equals(this.actions, ((PushOperation) o).actions);
  }

  /** Value class for per-field update operations. */
  record Action(PathMatchLocator locator, JsonNode value, boolean each, Integer position)
      implements ActionWithLocator {}
}
