package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import java.util.HashMap;
import java.util.Map;

/** List of update operator that's supported in update. */
public enum UpdateOperator {
  SET("$set"),
  UNSET("$unset");

  private String operator;

  UpdateOperator(String operator) {
    this.operator = operator;
  }

  private static Map<String, UpdateOperator> operatorMap = new HashMap<>();

  static {
    for (UpdateOperator updateOperator : UpdateOperator.values()) {
      operatorMap.put(updateOperator.operator, updateOperator);
    }
  }

  public static UpdateOperator getUpdateOperator(String operator) {
    return operatorMap.get(operator);
  }
}
