package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import com.fasterxml.jackson.databind.node.JsonNodeType;

/** Data type for a JSON value, used for de/encoding and storage. */
public enum JsonType {
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

  /*
     Return supported Filter Json types for a value
  */
  public static JsonType typeForValue(Object value) {
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
    throw new RuntimeException(String.format("Unsupported filter value type %s", value));
  }
}
