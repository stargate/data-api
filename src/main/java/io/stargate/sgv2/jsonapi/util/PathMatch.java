package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Objects;

/**
 * Definition of a target match for an action, such as ones contained by {@link
 * io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation}, or performed as part
 * of Projection or in-memory Sort processing. Instances are created by {@link PathMatchLocator}
 * based on locator's configuration (decoded from "Dotted path") and JSON document to evalute
 * against.
 */
public record PathMatch(
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
  public static PathMatch missingPath(String fullPath) {
    return new PathMatch(fullPath, null, null, null, -1);
  }

  public static PathMatch pathViaArray(
      String fullPath, JsonNode contextNode, JsonNode valueNode, int index) {
    return new PathMatch(fullPath, contextNode, valueNode, null, index);
  }

  public static PathMatch pathViaObject(
      String fullPath, JsonNode contextNode, JsonNode valueNode, String property) {
    return new PathMatch(fullPath, contextNode, valueNode, property, -1);
  }

  /**
   * Method that may be called to remove value node from its context, if there is value; value
   * removed (if any) is returned.
   */
  public JsonNode removeValue() {
    if (valueNode != null) {
      Objects.requireNonNull(contextNode);

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

  /**
   * Method that may be called to replace-or-insert (upsert?) a value at context this target points
   * to: if there was a value, it will be replaced; if not, new value will be inserted. In case of
   * Arrays, possible padding may be added in case of insertion to put it in specified index.
   *
   * @param newValue Value to upsert
   * @return Previous value at target, if any; {@code null} if none.
   */
  public JsonNode replaceValue(JsonNode newValue) {
    Objects.requireNonNull(contextNode);

    // Either Object property or Array element, depending on context
    if (contextNode.isObject()) {
      ((ObjectNode) contextNode).set(lastProperty, newValue);
    } else {
      ArrayNode array = (ArrayNode) contextNode;
      // important: it is legal to append beyond end; but if so, MUST pad with nulls
      while (lastIndex >= array.size()) {
        array.addNull();
      }
      ((ArrayNode) contextNode).set(lastIndex, newValue);
    }
    return valueNode;
  }
}
