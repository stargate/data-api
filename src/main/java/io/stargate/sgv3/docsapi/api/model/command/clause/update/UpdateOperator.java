package io.stargate.sgv3.docsapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import java.util.HashMap;
import java.util.Map;

/** List of update operator that's supported in update. */
public enum UpdateOperator {
  // First operators that are supported

  SET("$set") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return SetOperation.construct(arguments);
    }
  },
  UNSET("$unset") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return UnsetOperation.construct(arguments);
    }
  },

  // Then operators that we recognize but do not (yet) support

  INCR("$incr");

  private String operator;

  UpdateOperator(String operator) {
    this.operator = operator;
  }

  public UpdateOperation resolveOperation(ObjectNode arguments) {
    throw new DocsException(
        ErrorCode.UNSUPPORTED_UPDATE_OPERATION,
        "Unsupported update operator '%s'".formatted(operator));
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
