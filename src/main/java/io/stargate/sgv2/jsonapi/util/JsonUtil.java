package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class JsonUtil {
  /**
   * Method that compares to JSON values for equality using Mongo semantics which are otherwise same
   * as for JSON but additionally require exact ordering of Object (sub-document) values.
   *
   * @param node1 First JSON value to compare
   * @param node2 Second JSON value to compare
   * @return True if JSON values are equal according to Mongo semantics; false otherwise.
   */
  public static boolean equalsOrdered(JsonNode node1, JsonNode node2) {
    JsonNodeType type = node1.getNodeType();
    if (node2.getNodeType() != type) {
      return false;
    }
    switch (type) {
      case ARRAY -> {
        if (node1.size() != node2.size()) {
          return false;
        }
        Iterator<JsonNode> it = node1.elements();
        for (JsonNode value : node2) {
          if (!equalsOrdered(value, it.next())) {
            return false;
          }
        }
        return true;
      }
      case OBJECT -> {
        if (node1.size() != node2.size()) {
          return false;
        }
        ObjectNode ob1 = (ObjectNode) node1;
        ObjectNode ob2 = (ObjectNode) node2;
        Iterator<Map.Entry<String, JsonNode>> it1 = ob1.fields();
        Iterator<Map.Entry<String, JsonNode>> it2 = ob2.fields();

        while (it1.hasNext()) {
          Map.Entry<String, JsonNode> entry1 = it1.next();
          Map.Entry<String, JsonNode> entry2 = it2.next();

          if (!entry1.getKey().equals(entry2.getKey())
              || !equalsOrdered(entry1.getValue(), entry2.getValue())) {
            return false;
          }
        }
        return true;
      }
    }
    // For other nodes default equals() works fine:
    return Objects.equals(node1, node2);
  }
}
