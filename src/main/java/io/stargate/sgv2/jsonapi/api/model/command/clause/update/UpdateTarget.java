package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Definition of a target for an {@link UpdateOperation}; built from "dot path" and document to
 * update, by {@link UpdateTargetLocator}.
 */
public record UpdateTarget(
    String fullPath, JsonNode contextNode, JsonNode valueNode, String lastProperty, int lastIndex) {
  public static UpdateTarget missingPath(String fullPath) {
    return new UpdateTarget(fullPath, null, null, null, -1);
  }

  public static UpdateTarget pathViaArray(
      String fullPath, JsonNode contextNode, JsonNode valueNode, int index) {
    return new UpdateTarget(fullPath, contextNode, valueNode, null, index);
  }

  public static UpdateTarget pathViaObject(
      String fullPath, JsonNode contextNode, JsonNode valueNode, String property) {
    return new UpdateTarget(fullPath, contextNode, valueNode, property, -1);
  }

  public boolean hasContext() {
    return contextNode != null;
  }

  public boolean hasValue() {
    return valueNode != null;
  }

  /**
   * Method that may be called to remove value node from its context, if there is value; value
   * removed (if any) is returned.
   */
  public JsonNode removeValue() {
    if (valueNode != null) {
      // Either Object property or Array element, depending on context
      if (contextNode.isObject()) {
        ((ObjectNode) contextNode).remove(lastProperty);
      } else {
        // Important: to avoid changing indexes of other entries, MUST insert 'null' value
        // (case where index is past end do not have valueNode)
        ((ArrayNode) contextNode).setNull(lastIndex);
      }
    }
    return valueNode;
  }
}
