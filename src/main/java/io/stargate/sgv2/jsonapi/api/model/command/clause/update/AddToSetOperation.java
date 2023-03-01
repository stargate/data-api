package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@code $addToSet} update operation used to add distinct values in array fields
 * of documents.
 */
public class AddToSetOperation extends UpdateOperation {
  private List<AddToSetAction> actions;

  private AddToSetOperation(List<AddToSetAction> actions) {
    this.actions = actions;
  }

  public static AddToSetOperation construct(ObjectNode args) {
    Iterator<Map.Entry<String, JsonNode>> fieldIter = args.fields();

    List<AddToSetAction> updates = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      final String name = validateUpdatePath(UpdateOperator.ADD_TO_SET, entry.getKey());
      // At main level we must have field name (no modifiers)
      if (looksLikeModifier(name)) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                + ": $addToSet requires field names at main level, found modifier: "
                + name);
      }
      // But within field value modifiers are allowed: if there's one, all must be modifiers
      JsonNode value = entry.getValue();
      AddToSetAction action;
      if (value.isObject() && hasModifier((ObjectNode) value)) {
        action = buildActionWithModifiers(name, (ObjectNode) value);
      } else {
        action = new AddToSetAction(name, entry.getValue(), false);
      }
      updates.add(action);
    }
    return new AddToSetOperation(updates);
  }

  private static AddToSetAction buildActionWithModifiers(String propName, ObjectNode actionDef) {
    // We really only support "$each" but traverse in case more added in future
    JsonNode eachArg = null;

    Iterator<Map.Entry<String, JsonNode>> it = actionDef.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      final String modifier = entry.getKey();
      final JsonNode arg = entry.getValue();

      switch (modifier) {
        case "$each":
          eachArg = arg;
          if (!eachArg.isArray()) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
                ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                    + ": $addToSet modifier $each requires ARRAY argument, found: "
                    + eachArg.getNodeType());
          }
          break;

        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER,
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.getMessage()
                  + ": $addToSet only supports $each modifier; trying to use '"
                  + modifier
                  + "'");
      }
    }
    // For now should not be possible to occur but once we add other modifiers could:
    if (eachArg == null) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
          ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
              + ": $addToSet modifiers can only be used with $each modifier; none included");
    }

    return new AddToSetAction(propName, eachArg, true);
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
  public boolean updateDocument(ObjectNode doc, UpdateTargetLocator targetLocator) {
    boolean modified = false;
    for (AddToSetAction action : actions) {
      final String path = action.path;
      final JsonNode toAdd = action.value;

      UpdateTarget target = targetLocator.findOrCreate(doc, path);
      JsonNode node = target.valueNode();

      ArrayNode array;

      if (node == null) { // No such property? Add new array
        array = doc.arrayNode();
        target.replaceValue(array);
      } else if (node.isArray()) { // Already array? Append
        array = (ArrayNode) node;
      } else { // Something else? fail
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET.getMessage()
                + ": $addToSet requires target to be ARRAY; value at '"
                + path
                + "' of type "
                + node.getNodeType());
      }

      if (action.each) {
        for (JsonNode element : toAdd) {
          modified |= addToSet(array, element);
        }
      } else {
        modified |= addToSet(array, toAdd);
      }
    }

    // Every valid update operation modifies document so need just one:
    return modified;
  }

  private boolean addToSet(ArrayNode set, JsonNode elementToAdd) {
    // At first just... append
    set.add(elementToAdd);
    return true;
  }

  /** Value class for per-field update operations. */
  private record AddToSetAction(String path, JsonNode value, boolean each) {}
}
