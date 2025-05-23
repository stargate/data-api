package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
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
    Iterator<Map.Entry<String, JsonNode>> fieldIter = args.fields();

    List<Action> updates = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      // First verify update operation allowed for path (not for _id, $lexical/$vector/$vectorize)
      final String name = validateUpdatePath(UpdateOperator.PUSH, entry.getKey());
      // At main level we must have field name (no modifiers)
      if (looksLikeModifier(name)) {
        throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
            "$push requires field names at main level, found modifier: %s", name);
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

    Iterator<Map.Entry<String, JsonNode>> it = actionDef.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      final String modifier = entry.getKey();
      final JsonNode arg = entry.getValue();

      switch (modifier) {
        case "$each":
          eachArg = arg;
          if (!eachArg.isArray()) {
            throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
                "$push modifier $each requires ARRAY argument, found: %s", eachArg.getNodeType());
          }
          break;
        case "$position":
          // Mongo requires number
          if (!arg.isNumber()) {
            throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
                "$push modifier $position requires (integral) NUMBER argument, found: %s",
                arg.getNodeType());
          }
          // but floating-point won't do either:
          if (!arg.isIntegralNumber()) {
            throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
                "$push modifier $position requires Integer NUMBER argument, instead got: %s",
                arg.asText());
          }
          position = arg.intValue();
          break;

        default:
          throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.toApiException(
              "$push only supports $each and $position currently; trying to use '%s'", modifier);
      }
    }
    // For now should not be possible to occur but once we add other modifiers could:
    if (eachArg == null) {
      throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
          "$push modifiers can only be used with $each modifier; none included");
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
        throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_TARGET.toApiException(
            "$push requires target to be ARRAY; value at '%s' of type %s",
            target.fullPath(), node.getNodeType());
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
