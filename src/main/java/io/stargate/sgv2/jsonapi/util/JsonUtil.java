package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class JsonUtil {
  public static final String EJSON_VALUE_KEY_DATE = "$date";

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

  /**
   * Helper method that may be used to see if given JSON Value looks like it is an EJSON-encoded
   * value -- that is, JSON Object with 1 entry, name of which starts with "$" character -- without
   * decoding value. No validation is done beyond feasibility: caller is likely to want to call
   * other methods like {@link #extractEJsonDate} for further processing and validation/
   *
   * @param json JSON value to check
   * @return {@code true} if value looks like it has to be EJSON value (valid or invalid); {@code
   *     false} if not.
   */
  public static boolean looksLikeEJsonValue(JsonNode json) {
    return json.isObject() && json.size() == 1 && json.fieldNames().next().startsWith("$");
  }

  /**
   * Helper method that will see if given {@link JsonNode} is an EJSON-encoded "Date" (aka
   * Timestamp) value and if so, constructs and returns matching {@link java.util.Date} value. See
   * {@href https://documents.meteor.com/api/ejson.html} for details on encoded value.
   *
   * @param json JSON value to check
   * @return Date extracted, if given valid EJSON-encoded Date value; or {@code null} if not
   *     EJSON-like; or {@link JsonApiException} for malformed EJSON value
   * @throws JsonApiException If value indicates it would be EJSON-encoded date (by key) but has
   *     invalid value part (not number)
   */
  public static Date extractEJsonDate(JsonNode json, Object path) {
    if (json.isObject() && json.size() == 1) {
      JsonNode value = json.get(EJSON_VALUE_KEY_DATE);
      if (value != null) {
        if (value.isIntegralNumber() && value.canConvertToLong()) {
          return new Date(value.longValue());
        }
        // Otherwise we have an error case
        throw new JsonApiException(
            ErrorCode.SHRED_BAD_EJSON_VALUE,
            String.format(
                "%s: Date (%s) needs to have NUMBER value, has %s (path '%s')",
                ErrorCode.SHRED_BAD_EJSON_VALUE.getMessage(),
                EJSON_VALUE_KEY_DATE,
                value.getNodeType(),
                path));
      }
    }
    return null;
  }

  /**
   * Similar to {@link #extractEJsonDate(JsonNode, Object)} but will not throw an exception in any
   * case (simply returns {@code null})
   */
  public static Date tryExtractEJsonDate(JsonNode json) {
    if (json.isObject() && json.size() == 1) {
      JsonNode value = json.get(EJSON_VALUE_KEY_DATE);
      if (value != null) {
        if (value.isIntegralNumber() && value.canConvertToLong()) {
          return new Date(value.longValue());
        }
      }
    }
    return null;
  }

  public static ObjectNode createEJSonDate(JsonNodeCreator f, Date date) {
    return createEJSonDate(f, date.getTime());
  }

  public static ObjectNode createEJSonDate(JsonNodeCreator f, long timestamp) {
    ObjectNode json = f.objectNode();
    json.put(EJSON_VALUE_KEY_DATE, timestamp);
    return json;
  }

  public static Map<String, Object> createEJSonDateAsMap(long timestamp) {
    return Map.of(EJSON_VALUE_KEY_DATE, timestamp);
  }
}
