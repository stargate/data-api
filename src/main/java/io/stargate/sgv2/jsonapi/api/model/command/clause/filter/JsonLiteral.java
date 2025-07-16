package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

// Literal value to use as RHS operand in the query

import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.bson.types.ObjectId;

/**
 * @param value Literal value to use as RHS operand in the query
 * @param type Literal value type of the RHS operand in the query
 * @param <T> Data type of the value, the value should be the Java object value extracted from the
 *     Jackson node.
 */
public record JsonLiteral<T>(T value, JsonType type) {

  /**
   * Create Typed JsonLiteral to wrap the supplied Java object
   *
   * @param value the Java object, extracted from JSON document, to wrap.
   * @return {@link JsonLiteral}.
   */
  public static JsonLiteral<?> wrap(Object value) {
    return switch (value) {
      case null -> new JsonLiteral<>(null, JsonType.NULL);
      case DocumentId id -> new JsonLiteral<>(id, JsonType.DOCUMENT_ID);
      case BigDecimal bd -> new JsonLiteral<>(bd, JsonType.NUMBER);
      case Boolean bool -> new JsonLiteral<>(bool, JsonType.BOOLEAN);
      case Date date -> new JsonLiteral<>(date, JsonType.DATE);
      case String str -> new JsonLiteral<>(str, JsonType.STRING);
      case List<?> list -> new JsonLiteral<>((List<Object>) list, JsonType.ARRAY);
      case Map<?, ?> map -> new JsonLiteral<>(map, JsonType.SUB_DOC);
      case UUID uuid -> new JsonLiteral<>(uuid.toString(), JsonType.STRING);
      case ObjectId oid -> new JsonLiteral<>(oid.toString(), JsonType.STRING);
      case byte[] bytes -> new JsonLiteral<>(bytes, JsonType.EJSON_WRAPPER);
      default -> // no match should not happen in prod, we can throw a runtime exception
          throw new IllegalArgumentException(
              "JsonLiteral.wrap() - unknown value type: " + value.getClass().getName());
    };
  }

  // Overridden to help figure out unit test failures (wrt type of 'value')
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("JsonLiteral{type=").append(type);
    sb.append(", value");
    if (value == null) {
      sb.append("=null");
    } else if (type == JsonType.ARRAY) {
      // vectors are long arrays, do not need the full array printed
      sb.append("(").append(value.getClass().getSimpleName()).append(")=");
      var subList = ((List) value).subList(0, Math.min(5, ((List) value).size()));
      sb.append(subList);
    } else {
      sb.append("(").append(value.getClass().getSimpleName()).append(")=");
      sb.append(value);
    }
    sb.append("}");
    return sb.toString();
  }
}
