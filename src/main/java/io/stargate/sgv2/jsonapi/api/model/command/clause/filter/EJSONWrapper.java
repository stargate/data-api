package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/**
 * Intermediate type for further processing: used with {@link JsonLiteral} and {@link
 * JsonType#EJSON_WRAPPER} to store special types in JSON (e.g. binary data). Currently only one
 * type is supported but this can be extended in the future.
 */
public class EJSONWrapper {
  // Actual DRY keys
  public static final String $BINARY = "$binary";

  public enum EJSONType {
    BINARY($BINARY);

    private final String key;

    EJSONType(String key) {
      this.key = key;
    }

    public static EJSONType fromKey(String key) {
      // With very few entries just do this: can build lookup Map if needed
      // in future
      return switch (key) {
        case $BINARY -> BINARY;
        default -> null;
      };
    }

    public String key() {
      return key;
    }
  }

  private final EJSONType type;
  private final JsonNode value;

  public EJSONWrapper(EJSONType type, JsonNode value) {
    this.type = Objects.requireNonNull(type);
    this.value = Objects.requireNonNull(value);
  }

  public static EJSONWrapper maybeFrom(ObjectNode objectNode) {
    // EJSON-wrapper: single entry with key starting with "$TYPE"?
    if (objectNode.size() == 1) {
      Map.Entry<String, JsonNode> entry = objectNode.fields().next();
      return maybeFrom(entry.getKey(), entry.getValue());
    }
    return null;
  }

  public static EJSONWrapper maybeFrom(String key, JsonNode value) {
    EJSONType type = EJSONType.fromKey(key);
    return (type == null) ? null : new EJSONWrapper(type, value);
  }

  public static EJSONWrapper binaryWrapper(ByteBuffer value) {
    return binaryWrapper(bytesFromByteBuffer(value));
  }

  public static EJSONWrapper binaryWrapper(byte[] value) {
    return new EJSONWrapper(EJSONType.BINARY, JsonNodeFactory.instance.binaryNode(value));
  }

  public EJSONType type() {
    return type;
  }

  public JsonNode value() {
    return value;
  }

  @Override
  public String toString() {
    // Include a snippet of actual value for easier debugging
    String valueDesc = String.valueOf(value);
    if (valueDesc.length() > 20) {
      valueDesc = valueDesc.substring(0, 20) + "[...]";
    }
    return "EJSONWrapper{%s,(%s)%s}".formatted(type.key(), value.getNodeType().name(), valueDesc);
  }

  // Return value specifies how serialization works: re-creates wrapper
  @JsonValue
  public JsonNode asJsonNode() {
    return JsonNodeFactory.instance.objectNode().set(type.key(), value);
  }

  private static byte[] bytesFromByteBuffer(ByteBuffer buffer) {
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return bytes;
  }
}
