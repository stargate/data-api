package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.HashMap;
import java.util.Map;

/** List of update operator that's supported in update. */
public enum UpdateOperator {
  ADD_TO_SET("$addToSet") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return AddToSetOperation.construct(arguments);
    }
  },

  CURRENT_DATE("$currentDate") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return CurrentDateOperation.construct(arguments);
    }
  },

  INC("$inc") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return IncOperation.construct(arguments);
    }
  },

  MAX("$max") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return MinMaxOperation.constructMax(arguments);
    }
  },

  MIN("$min") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return MinMaxOperation.constructMin(arguments);
    }
  },

  MUL("$mul") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return MulOperation.construct(arguments);
    }
  },

  POP("$pop") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return PopOperation.construct(arguments);
    }
  },

  PUSH("$push") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return PushOperation.construct(arguments);
    }
  },

  RENAME("$rename") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return RenameOperation.construct(arguments);
    }
  },

  SET("$set") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return SetOperation.constructSet(arguments);
    }
  },
  SET_ON_INSERT("$setOnInsert") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return SetOperation.constructSetOnInsert(arguments);
    }
  },

  UNSET("$unset") {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return UnsetOperation.construct(arguments);
    }
  },

  // Then operators that we recognize but do not (yet) support

  PULL_ALL("$pullAll");

  private String operator;

  UpdateOperator(String operator) {
    this.operator = operator;
  }

  public String operator() {
    return operator;
  }

  public UpdateOperation resolveOperation(ObjectNode arguments) {
    throw new JsonApiException(
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
