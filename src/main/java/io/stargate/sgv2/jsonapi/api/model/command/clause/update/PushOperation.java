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
      // 06-Feb-2023, tatu: Until "$each" supported, verify that no modifiers included
      JsonNode value = entry.getValue();
      String firstModifier = findModifier(value);
      if (firstModifier != null) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_MODIFIER.getMessage()
                + ": $push does not yet support modifiers; trying to use "
                + firstModifier);
      }
      updates.add(new PushAction(entry.getKey(), entry.getValue()));
    }
    return new PushOperation(updates);
  }

  private static String findModifier(JsonNode node) {
    if (node.isObject()) {
      // Sigh. Wish things were not returned as iterators by JsonNode...
      Iterator<String> it = node.fieldNames();
      while (it.hasNext()) {
        String name = it.next();
        if (name.startsWith("$")) {
          return name;
        }
      }
    }
    return null;
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    for (PushAction update : updates) {
      final String path = update.path;
      final JsonNode toAdd = update.value;
      JsonNode node = doc.get(path);
      if (node == null) { // No such property? Add new 1-element array
        doc.putArray(path).add(toAdd);
      } else if (node.isArray()) { // Already array? Append
        ((ArrayNode) node).add(toAdd);
      } else { // Something else? fail
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_TARGET.getMessage()
                + ": $push requires target to be Array; value at '"
                + path
                + " of type "
                + node.getNodeType());
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

  /**
   * Value class for per-field update operations: initially simple replacement but will need
   * different value type soon to allow {@code $each modifier}.
   */
  private record PushAction(String path, JsonNode value) {}
}
