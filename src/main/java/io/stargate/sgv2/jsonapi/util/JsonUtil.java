package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Utf8;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.JsonExtensionType;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import org.bson.types.ObjectId;

public class JsonUtil {
  @Deprecated // use JsonExtensionType.EJSON_DATE.encodedName() instead
  public static final String EJSON_VALUE_KEY_DATE = JsonExtensionType.EJSON_DATE.encodedName();

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
   * {@href https://docs.meteor.com/api/ejson.html} for details on encoded value.
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
        throw ErrorCode.SHRED_BAD_EJSON_VALUE.toApiException(
            "Date (%s) needs to have NUMBER value, has %s (path '%s')",
            EJSON_VALUE_KEY_DATE, value.getNodeType(), path);
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

  public static Date createDateFromDocumentId(DocumentId documentId) {
    return new Date((Long) ((Map) documentId.value()).get(EJSON_VALUE_KEY_DATE));
  }

  public static JsonExtensionType findJsonExtensionType(JsonNode jsonValue) {
    if (jsonValue.isObject() && jsonValue.size() == 1) {
      String fieldName = jsonValue.fieldNames().next();
      return JsonExtensionType.fromEncodedName(fieldName);
    }
    return null;
  }

  public static JsonExtensionType findJsonExtensionType(String encodedType) {
    return JsonExtensionType.fromEncodedName(encodedType);
  }

  public static Object extractExtendedValue(JsonExtensionType etype, JsonNode valueWrapper) {
    Object value = tryExtractExtendedValue(etype, valueWrapper);
    if (value == null) {
      failOnInvalidExtendedValue(etype, valueWrapper.iterator().next());
    }
    return value;
  }

  public static Object extractExtendedValue(
      JsonExtensionType etype, Map.Entry<String, JsonNode> valueEntry) {
    Object value = tryExtractExtendedValue(etype, valueEntry);
    if (value == null) {
      failOnInvalidExtendedValue(etype, valueEntry.getValue());
    }
    return value;
  }

  public static Object extractExtendedValueUnwrapped(
      JsonExtensionType etype, JsonNode unwrappedValue) {
    Object value = tryExtractExtendedValue(etype, unwrappedValue);
    if (value == null) {
      failOnInvalidExtendedValue(etype, unwrappedValue);
    }
    return value;
  }

  public static Object tryExtractExtendedValue(JsonExtensionType etype, JsonNode valueWrapper) {
    // Caller should have verified that we have a single-field Object; but double check
    if (valueWrapper.isObject() && valueWrapper.size() == 1) {
      return tryExtractExtendedFromUnwrapped(etype, valueWrapper.iterator().next());
    }
    return null;
  }

  public static Object tryExtractExtendedValue(
      JsonExtensionType etype, Map.Entry<String, JsonNode> valueEntry) {
    return tryExtractExtendedFromUnwrapped(etype, valueEntry.getValue());
  }

  private static Object tryExtractExtendedFromUnwrapped(JsonExtensionType etype, JsonNode value) {
    switch (etype) {
      case EJSON_DATE:
        if (value.isIntegralNumber() && value.canConvertToLong()) {
          return new Date(value.longValue());
        }
        break;
      case OBJECT_ID:
        try {
          return new ObjectId(value.asText());
        } catch (IllegalArgumentException e) {
        }
        break;
      case UUID:
        try {
          return java.util.UUID.fromString(value.asText());
        } catch (IllegalArgumentException e) {
        }
        break;
    }
    return null;
  }

  private static void failOnInvalidExtendedValue(JsonExtensionType etype, JsonNode value) {
    switch (etype) {
      case EJSON_DATE:
        throw ErrorCode.SHRED_BAD_EJSON_VALUE.toApiException(
            "'%s' value has to be an epoch timestamp, instead got (%s)",
            etype.encodedName(), value);
      case OBJECT_ID:
        throw ErrorCode.SHRED_BAD_EJSON_VALUE.toApiException(
            "'%s' value has to be 24-digit hexadecimal ObjectId, instead got (%s)",
            etype.encodedName(), value);
      case UUID:
        throw ErrorCode.SHRED_BAD_EJSON_VALUE.toApiException(
            "'%s' value has to be 36-character UUID String, instead got (%s)",
            etype.encodedName(), value);
    }
  }

  /**
   * Utility method to check whether UTF-8 encoded length of given String is above given maximum
   * length, and if so, return actual length (in bytes); otherwise return {@code
   * OptionalInt.empty()}. This is faster than encoding String as byte[] and checking length as it
   * not only avoids unnecessary allocation of potentially long String, but also avoids exact
   * calculation for shorter Strings where we can determine that size cannot exceed the limit (since
   * we know that UTF-8 encoded length is at most 3x of char length).
   *
   * @param value String to check
   * @param maxLengthInBytes Maximum length in bytes allowed (inclusive)
   * @return {@code OptionalInt.empty()} if {@code value} is at most {@code maxLengthInBytes};
   *     actual length if above {@code maxLengthInBytes}.
   */
  public static OptionalInt lengthInBytesIfAbove(String value, int maxLengthInBytes) {
    // First, a quick and cheap check: maximum expansion is 3x, so avoid check if
    // we know it cannot expand enough to be too long
    final int charLen = value.length();
    if ((charLen * 3) > maxLengthInBytes) {
      // Otherwise calculate actual length, but avoid byte[] allocation:
      int byteLen = Utf8.encodedLength(value);
      if (byteLen > maxLengthInBytes) {
        return OptionalInt.of(byteLen);
      }
    }
    return OptionalInt.empty();
  }
}
