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
import java.util.Objects;

/**
 * Implementation of {@code $push} update operation used to append values in array fields of
 * documents.
 */
public class PushOperation extends UpdateOperation {
  private List<PushAction> updates;

  private PushOperation(List<PushAction> updates) {
    this.updates = updates;
  }

  public static PushOperation construct(ObjectNode args) {
    Iterator<Map.Entry<String, JsonNode>> fieldIter = args.fields();

    // We'll collect updates into List since in near future they will be more complicated than
    // Just path/value pairs (to support "$each" modifier)
    List<PushAction> updates = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      // At main level we must have field name (no modifiers)
      final String name = entry.getKey();
      if (looksLikeModifier(name)) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                + ": $push requires field names at main level, found modifier: "
                + name);
      }
      // But within field value modifiers are allowed: if there's one, all must be modifiers
      JsonNode value = entry.getValue();
      PushAction action;
      if (value.isObject() && hasModifier((ObjectNode) value)) {
        action = buildActionWithModifiers(name, (ObjectNode) value);
      } else {
        action = new PushAction(name, entry.getValue(), false, null);
      }
      updates.add(action);
    }
    return new PushOperation(updates);
  }

  private static PushAction buildActionWithModifiers(String propName, ObjectNode actionDef) {
    // We know there is at least one modifier; and if so, all must be modifiers.
    // For now we only support "$each"
    JsonNode eachArg = null;
    Integer position = null;

    Iterator<Map.Entry<String, JsonNode>> it = actionDef.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      final String modifier = entry.getKey();
      final JsonNode arg = entry.getValue();

      // Support $each and $position
      switch (modifier) {
        case "$each":
          eachArg = arg;
          if (!eachArg.isArray()) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
                ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                    + ": $push modifier $each requires ARRAY argument, found: "
                    + eachArg.getNodeType());
          }
          break;
        case "$position":
          // Mongo requires number
          if (!arg.isNumber()) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
                ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                    + ": $push modifier $position requires (integral) NUMBER argument, found: "
                    + arg.getNodeType());
          }
          // but floating-point won't do either:
          if (!arg.isIntegralNumber()) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
                ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
                    + ": $push modifier $position requires Integer NUMBER argument, instead got: "
                    + arg.asText());
          }
          position = arg.intValue();
          break;

        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER,
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.getMessage()
                  + ": $push only supports $each and $position currently; trying to use '"
                  + modifier
                  + "'");
      }
    }
    // For now should not be possible to occur but once we add other modifiers could:
    if (eachArg == null) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM,
          ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PARAM.getMessage()
              + ": $push modifiers can only be used with $each modifier; none included");
    }

    return new PushAction(propName, eachArg, true, position);
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
  public boolean updateDocument(ObjectNode doc) {
    for (PushAction update : updates) {
      final String path = update.path;
      final JsonNode toAdd = update.value;
      JsonNode node = doc.get(path);
      ArrayNode array;

      if (node == null) { // No such property? Add new 1-element array
        array = doc.putArray(path);
      } else if (node.isArray()) { // Already array? Append
        array = (ArrayNode) node;
      } else { // Something else? fail
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET.getMessage()
                + ": $push requires target to be Array; value at '"
                + path
                + "' of type "
                + node.getNodeType());
      }
      // Regular add or $each?
      if (update.each) {
        for (JsonNode element : toAdd) {
          array.add(element);
        }
      } else {
        array.add(toAdd);
      }
    }

    // Every valid update operation modifies document so need just one:
    return !updates.isEmpty();
  }

  // Just needed for tests
  @Override
  public boolean equals(Object o) {
    return (o instanceof PushOperation)
        && Objects.equals(this.updates, ((PushOperation) o).updates);
  }

  /** Value class for per-field update operations. */
  private record PushAction(String path, JsonNode value, boolean each, Integer position) {}
}
