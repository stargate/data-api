package io.stargate.sgv3.docsapi.service.shredding.model;

import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.common.base.Functions;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** Data type for a JSON value, used for de/encoding and storage. */
public enum JsonType {
  UNKNOWN(-1, JsonNodeType.MISSING), // TODO : Ask Tatu how missing is used
  BOOLEAN(1, JsonNodeType.BOOLEAN),
  NUMBER(2, JsonNodeType.NUMBER),
  STRING(3, JsonNodeType.STRING),
  NULL(4, JsonNodeType.NULL),
  SUB_DOC(5, JsonNodeType.OBJECT),
  ARRAY(6, JsonNodeType.ARRAY);

  public final int value;
  public final JsonNodeType jacksonType;

  private JsonType(int value, JsonNodeType jacksonType) {
    this.value = value;
    this.jacksonType = jacksonType;
  }

  private static final Map<Integer, JsonType> valueMap;
  private static final Map<JsonNodeType, JsonType> jacksonMap;

  static {
    valueMap =
        Arrays.stream(values()).collect(Collectors.toMap(e -> e.value, Functions.identity()));

    jacksonMap =
        Arrays.stream(values()).collect(Collectors.toMap(e -> e.jacksonType, Functions.identity()));
  }

  public static JsonType fromValue(Integer value) {

    JsonType element = valueMap.get(value);
    if (element != null) {
      return element;
    }
    throw new RuntimeException("Known Json Type value " + value.toString());
  }

  public static JsonType fromJacksonType(JsonNodeType nodeType) {

    JsonType element = jacksonMap.get(nodeType);
    if (element != null) {
      return element;
    }
    throw new RuntimeException("Known Json Type jacksonType " + nodeType.toString());
  }

  public static JsonType typeForValue(Object value) {
    // Still need this and the jackson map for handling creating filter clause in code where we dont
    // have the JsonNode from jackson

    if (value instanceof String) {
      return JsonType.STRING;
    }
    if (value instanceof Number) {
      return JsonType.NUMBER;
    }
    if (value instanceof Boolean) {
      return JsonType.BOOLEAN;
    }
    if (value == null) {
      return JsonType.NULL;
    }

    throw new RuntimeException(String.format("Unknown type %s", value));
  }
}
