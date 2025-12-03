package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import io.stargate.sgv2.jsonapi.util.PathMatch;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@code $addToSet} update operation used to add distinct values in array fields
 * of documents.
 */
public class AddToSetOperation extends UpdateOperation<AddToSetOperation.Action> {
  private AddToSetOperation(List<Action> actions) {
    super(actions);
  }

  public static AddToSetOperation construct(ObjectNode args) {
    List<Action> updates = new ArrayList<>();
    for (Map.Entry<String, JsonNode> entry : args.properties()) {
      final String name = validateUpdatePath(UpdateOperator.ADD_TO_SET, entry.getKey());
      // At main level we must have field name (no modifiers)
      if (looksLikeModifier(name)) {
        throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
            "$addToSet requires field names at main level, found modifier: %s", name);
      }
      // But within field value modifiers are allowed: if there's one, all must be modifiers
      JsonNode value = entry.getValue();
      Action action;
      if (value.isObject() && hasModifier((ObjectNode) value)) {
        action = buildActionWithModifiers(name, (ObjectNode) value);
      } else {
        action = new Action(PathMatchLocator.forPath(name), entry.getValue(), false);
      }
      updates.add(action);
    }
    return new AddToSetOperation(updates);
  }

  private static Action buildActionWithModifiers(String propName, ObjectNode actionDef) {
    // We really only support "$each" but traverse in case more added in future
    JsonNode eachArg = null;

    for (Map.Entry<String, JsonNode> entry : actionDef.properties()) {
      final String modifier = entry.getKey();
      final JsonNode arg = entry.getValue();

      switch (modifier) {
        case "$each":
          eachArg = arg;
          if (!eachArg.isArray()) {
            throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
                "$addToSet modifier $each requires ARRAY argument, found: %s",
                eachArg.getNodeType());
          }
          break;

        default:
          throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.toApiException(
              "$addToSet only supports $each modifier; trying to use '%s'", modifier);
      }
    }
    // For now should not be possible to occur but once we add other modifiers could:
    if (eachArg == null) {
      throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION_PARAM.toApiException(
          "$addToSet modifiers can only be used with $each modifier; none included");
    }

    return new Action(PathMatchLocator.forPath(propName), eachArg, true);
  }

  private static boolean hasModifier(ObjectNode node) {
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
    boolean modified = false;
    for (Action action : actions) {
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
            "$addToSet requires target to be ARRAY; value at '%s' of type %s",
            action.locator().path(), node.getNodeType());
      }

      final JsonNode toAdd = action.value;
      if (action.each) {
        for (JsonNode element : toAdd) {
          modified |= addToSet(array, element);
        }
      } else {
        modified |= addToSet(array, toAdd);
      }
    }

    return new UpdateOperationResult(modified, List.of());
  }

  private boolean addToSet(ArrayNode set, JsonNode elementToAdd) {
    // See if Array already has value we are trying to add; if so, do NOT add
    for (JsonNode node : set) {
      if (JsonUtil.equalsOrdered(elementToAdd, node)) {
        return false;
      }
    }
    set.add(elementToAdd);
    return true;
  }

  /** Value class for per-field update operations. */
  record Action(PathMatchLocator locator, JsonNode value, boolean each)
      implements ActionWithLocator {}
}
