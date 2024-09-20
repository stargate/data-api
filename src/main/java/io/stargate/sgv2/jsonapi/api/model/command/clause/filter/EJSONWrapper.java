package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Intermediate type for further processing: used with {@link JsonLiteral} and {@link
 * JsonType#EJSON_WRAPPER} to store special types in JSON (e.g. binary data).
 * Currently only one type is supported but this can be extended in the future.
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

  public static EJSONWrapper from(String key, JsonNode value) {
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
    return "EJSONWrapper{%s}".formatted(type.key());
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
