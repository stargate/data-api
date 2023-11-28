package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.stargate.bridge.grpc.Values;
import io.stargate.sgv2.api.common.cql.builder.Literal;
import java.util.Objects;

/**
 * This class is an extension of the Literal class from
 * sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/cql/builder/Term.java This is
 * required as a placeholder to set values in query builder and extracted out to set the value in
 * SimpleStatement positional values
 */
public class JsonTerm extends Literal {
  static final String NULL_ERROR_MESSAGE = "Use Values.NULL to bind a null CQL value";
  private final Object key;
  private final Object value;

  public JsonTerm(Object value) {
    this(null, value);
  }

  public JsonTerm(Object key, Object value) {
    super(Values.NULL);
    this.key = key;
    this.value = value;
  }

  public Object getKey() {
    return this.key;
  }

  public Object getValue() {
    return this.value;
  }

  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof JsonTerm) {
      JsonTerm that = (JsonTerm) other;
      return Objects.equals(this.value, that.value) && Objects.equals(this.key, that.key);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[] {this.value, this.key});
  }
}
