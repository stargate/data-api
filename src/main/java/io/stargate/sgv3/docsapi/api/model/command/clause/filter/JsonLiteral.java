package io.stargate.sgv3.docsapi.api.model.command.clause.filter;

import java.math.BigDecimal;

// Literal value to use as RHS operand in the query
public record JsonLiteral(Object value, JsonType type) {
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
