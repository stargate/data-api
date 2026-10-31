package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import java.util.HashMap;
import java.util.Map;

/** List of update operator that's supported in update. */
public enum UpdateOperator {
  ADD_TO_SET(Names.ADD_TO_SET) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return AddToSetOperation.construct(arguments);
    }
  },

  CURRENT_DATE(Names.CURRENT_DATE) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return CurrentDateOperation.construct(arguments);
    }
  },

  INC(Names.INC) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return IncOperation.construct(arguments);
    }
  },

  MAX(Names.MAX) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return MinMaxOperation.constructMax(arguments);
    }
  },

  MIN(Names.MIN) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return MinMaxOperation.constructMin(arguments);
    }
  },

  MUL(Names.MUL) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return MulOperation.construct(arguments);
    }
  },

  POP(Names.POP) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return PopOperation.construct(arguments);
    }
  },

  PUSH(Names.PUSH) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return PushOperation.construct(arguments);
    }
  },

  RENAME(Names.RENAME) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return RenameOperation.construct(arguments);
    }
  },

  SET(Names.SET) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return SetOperation.constructSet(arguments);
    }
  },
  SET_ON_INSERT(Names.SET_ON_INSERT) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return SetOperation.constructSetOnInsert(arguments);
    }
  },

  UNSET(Names.UNSET) {
    @Override
    public UpdateOperation resolveOperation(ObjectNode arguments) {
      return UnsetOperation.construct(arguments);
    }
  },

  // Then operators that we recognize but do not (yet) support

  PULL_ALL(Names.PULL_ALL);

  public interface Names {
    String ADD_TO_SET = "$addToSet";
    String CURRENT_DATE = "$currentDate";
    String INC = "$inc";
    String MAX = "$max";
    String MIN = "$min";
    String MUL = "$mul";
    String POP = "$pop";
    String PUSH = "$push";
    String RENAME = "$rename";
    String SET = "$set";
    String SET_ON_INSERT = "$setOnInsert";
    String UNSET = "$unset";
    String PULL_ALL = "$pullAll";
  }

  private String apiName;

  UpdateOperator(String apiName) {
    this.apiName = apiName;
  }

  public String apiName() {
    return apiName;
  }

  public UpdateOperation resolveOperation(ObjectNode arguments) {
    throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION.get(
        Map.of("errorMessage", "Unsupported update operator '%s'".formatted(apiName)));
  }

  private static Map<String, UpdateOperator> operatorMap = new HashMap<>();

  static {
    for (UpdateOperator updateOperator : UpdateOperator.values()) {
      operatorMap.put(updateOperator.apiName, updateOperator);
    }
  }

  public static UpdateOperator getUpdateOperator(String operator) {
    return operatorMap.get(operator);
  }
}
