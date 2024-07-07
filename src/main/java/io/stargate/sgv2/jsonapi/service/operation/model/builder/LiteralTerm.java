package io.stargate.sgv2.jsonapi.service.operation.model.builder;

import java.util.List;

/**
 * A CQL literal term that we want to compare to a CQL column
 *
 * <p>e.g. the 7 in `SELECT * FROM table WHERE column = 7`
 */
public class LiteralTerm<T> extends BuildConditionTerm {

  private final T value;

  public LiteralTerm(T value) {
    this.value = value;
  }

  @Override
  public void appendPositionalValue(List<Object> values) {
    values.add(value);
  }
}
