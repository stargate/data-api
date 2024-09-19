package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Objects;

/**
 * Intermediate type for further processing: used with {@link JsonLiteral} and {@link
 * JsonType#EJSON_WRAPPER} to store special types in JSON (e.g. binary data, dates). Note that
 */
public class EJSONWrapper {
  public enum EJSONType {
    BINARY("$binary");

    private final String key;

    EJSONType(String key) {
      this.key = key;
    }

    public static EJSONType fromKey(String key) {
      // With very few entries just do this: can build lookup Map if needed
      // in future
      return switch (key) {
        case "$binary" -> BINARY;
        default -> null;
      };
    }

    public String getKey() {
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

  public EJSONType getType() {
    return type;
  }

  public JsonNode getValue() {
    return value;
  }
}
