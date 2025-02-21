package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

// Literal value to use as RHS operand in the query


import java.util.List;

/**
 * @param value Literal value to use as RHS operand in the query
 * @param type Literal value type of the RHS operand in the query
 * @param <T> Data type of the value, the value should be the Java object value extracted from the
 *     Jackson node.
 */
public record JsonLiteral<T>(T value, JsonType type) {
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
      var subList = ((List)value).subList(0, Math.min(5, ((List)value).size()));
      sb.append(subList);
    } else {
      sb.append("(").append(value.getClass().getSimpleName()).append(")=");
      sb.append(value);
    }
    sb.append("}");
    return sb.toString();
  }
}
