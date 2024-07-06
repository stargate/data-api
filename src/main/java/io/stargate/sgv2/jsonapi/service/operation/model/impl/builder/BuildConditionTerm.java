package io.stargate.sgv2.jsonapi.service.operation.model.impl.builder;

import java.util.List;

/**
 * See @link{BuiltCondition} this is the operand of the condition
 */
public abstract class BuildConditionTerm {

  /**
   * This method is used for populate positional cql value list e.g. select * from table where
   * map[?] = ? limit 1; For this case, we populate as key and value
   *
   * <p>e.g. select * from table where array_contains contains ? limit 1; * For this case, we
   * populate positional cql value
   */
  public abstract void appendPositionalValue(List<Object> values);
}
