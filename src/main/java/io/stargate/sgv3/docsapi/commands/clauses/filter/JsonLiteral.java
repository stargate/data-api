package io.stargate.sgv3.docsapi.commands.clauses.filter;

import io.stargate.sgv3.docsapi.shredding.JsonType;
import java.math.BigDecimal;

// Literal value to use as RHS operand in the query
public class JsonLiteral {
  // Type important, e.g. if text then we look for fields that are text.
  public final JsonType type;
  private final Object value;

  public JsonLiteral(JsonType type, Object value) {
    this.type = type;
    this.value = value;
  }

  public static JsonLiteral from(Object literal) {
    return new JsonLiteral(JsonType.typeForValue(literal), literal);
  }

  public Object getValue() {
    return value;
  }

  public <T> T getTypedValue() {
    switch (type) {
      case STRING:
        return (T) value;
      case NUMBER:
        // we want to get the value from the parameter into a Big Decimal, this code is ugly
        if (value instanceof Integer) {
          return (T) BigDecimal.valueOf((Integer) value);
        }
        if (value instanceof Long) {
          return (T) BigDecimal.valueOf((Long) value);
        }
        if (value instanceof Double) {
          return (T) BigDecimal.valueOf((Double) value).stripTrailingZeros();
        }
        if (value instanceof Float) {
          return (T) BigDecimal.valueOf((Float) value).stripTrailingZeros();
        }
        if (value instanceof BigDecimal) {
          return (T) ((BigDecimal) value).stripTrailingZeros();
        }
        throw new RuntimeException(
            String.format(
                "Unknown number type %s in getTypesValue()", value.getClass().toString()));
      case BOOLEAN:
        return (T) value;
      default:
        // amorton - this maybe could be just a 1 line function, the error is here to detect if
        // there is a case we have not considered.
        throw new RuntimeException(String.format("Unknown JsonType %s in getTypesValue", type));
    }
  }
}
