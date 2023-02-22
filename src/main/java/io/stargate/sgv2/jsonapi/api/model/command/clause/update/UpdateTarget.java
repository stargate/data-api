package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Definition of a target for an {@link UpdateOperation}; built from "dot path" and document to
 * update, by {@link UpdateTargetLocator}.
 */
public record UpdateTarget(
    /** Full path to target for error reporting purposes */
    String fullPath,
    /**
     * Parent node of the value if one exists or could exist at indicated path; {@code null} if
     * there is no complete path to parent of value node.
     */
    JsonNode contextNode,
    /**
     * Currently existing value at specified path, if any; {@code null} if not. If not {@code null}
     * then {@code contextNode} must also be non-{@code null} and one (but not both!) of {@code
     * lastProperty} and {@code lastIndex} must similarly exist.
     */
    JsonNode valueNode,
    /**
     * Last property name (segment) from {@code contextNode} to value that either exists or could be
     * added as Object property. Existence means that {@code contextNode} exists and is an Object
     * node.
     */
    String lastProperty,
    /**
     * Last Array index (non-negative) from {@code contextNode} to value that either exists or could
     * be added as Array element; {@code -1} otherwise (context node is not Array). Existence means
     * that {@code contextNode} exists and is an Array node.
     */
    int lastIndex) {
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
