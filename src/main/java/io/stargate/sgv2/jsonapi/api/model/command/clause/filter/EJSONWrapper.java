package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.util.CqlVectorUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

/**
 * Intermediate type for further processing: used with {@link JsonLiteral} and {@link
 * JsonType#EJSON_WRAPPER} to store special types in JSON (e.g. binary data).
 */
public class EJSONWrapper {
  // Actual DRY keys
  public static final String $BINARY = "$binary";

  public static final String $DATE = "$date";

  public enum EJSONType {
    BINARY($BINARY),
    DATE($DATE);

    private final String key;

    EJSONType(String key) {
      this.key = key;
    }

    /**
     * Lookup method to find enum value from key
     *
     * @param key String key to look up
     * @return Actual EJSONType enum value (if found) or {@code null} (if not found).
     */
    public static EJSONType fromKey(String key) {
      // With few entries just switch: build lookup Map if needed in future
      return switch (key) {
        case $BINARY -> BINARY;
        case $DATE -> DATE;
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
      Map.Entry<String, JsonNode> entry = objectNode.properties().iterator().next();
      return maybeFrom(entry.getKey(), entry.getValue());
    }
    return null;
  }

  public static EJSONWrapper maybeFrom(String key, JsonNode value) {
    EJSONType type = EJSONType.fromKey(key);
    return (type == null) ? null : new EJSONWrapper(type, value);
  }

  /** Factory method for constructing "$binary" EJSON Wrapper with given contents */
  public static EJSONWrapper binaryWrapper(ByteBuffer value) {
    return binaryWrapper(bytesFromByteBuffer(value));
  }

  /** Factory method for constructing "$binary" EJSON Wrapper with given contents */
  public static EJSONWrapper binaryWrapper(byte[] value) {
    return new EJSONWrapper(EJSONType.BINARY, JsonNodeFactory.instance.binaryNode(value));
  }

  /**
   * Factory method for constructing "$date" EJSON Wrapper to wrap given milliseconds (since epoch,
   * Java standard) timestamp: for compatibility with Collections
   */
  public static EJSONWrapper timestampWrapper(long timestamp) {
    return new EJSONWrapper(EJSONType.DATE, JsonNodeFactory.instance.numberNode(timestamp));
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

  // Returns the float array value from the binary EJSON wrapper
  public float[] getVectorValueForBinary() {
    if (type != EJSONType.BINARY) {
      throw new IllegalStateException(
          "Vector value can only be extracted from binary EJSON wrapper");
    }
    try {
      return CqlVectorUtil.bytesToFloats(value().binaryValue());
    } catch (IOException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid content in EJSON $binary wrapper, problem: %s".formatted(e.getMessage()));
    }
  }
}
