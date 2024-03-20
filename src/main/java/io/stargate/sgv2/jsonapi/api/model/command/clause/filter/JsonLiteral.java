package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

// Literal value to use as RHS operand in the query

/**
 * @param value Literal value to use as RHS operand in the query
 * @param type Literal value type of the RHS operand in the query
 * @param <T> Data type of the object
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
    } else {
      sb.append("(").append(value.getClass().getSimpleName()).append(")=");
      sb.append(value);
    }
    sb.append("}");
    return sb.toString();
  }
}
