package io.stargate.sgv2.jsonapi.service.operation.model.impl.builder;

import java.util.List;
import java.util.Objects;

/**
 * This class is an extension of the Literal class from
 * sgv2-quarkus-common/src/main/java/io/stargate/sgv2/api/common/cql/builder/Term.java This is
 * required as a placeholder to set values in query builder and extracted out to set the value in
 * SimpleStatement positional values
 */
public class JsonTerm {
  static final String NULL_ERROR_MESSAGE = "Use Values.NULL to bind a null CQL value";
  // TODO: explain why this is an object and not a string
  private final Object key;
  private final Object value;

  // TODO: the variable name is "value" but this ctor is to set the key !
  public JsonTerm(Object value) {
    this(null, value);
  }

  public JsonTerm(Object key, Object value) {
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

  /**
   * This method is used for populate positional cql value list e.g. select * from table where
   * map[?] = ? limit 1; For this case, we populate as key and value
   *
   * <p>e.g. select * from table where array_contains contains ? limit 1; * For this case, we
   * populate positional cql value
   */
  public void addToCqlValues(List<Object> values) {
    if (this.key != null) {
      values.add(this.key);
    }
    values.add(this.value);
  }
}
