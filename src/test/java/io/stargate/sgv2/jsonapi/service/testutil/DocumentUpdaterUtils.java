package io.stargate.sgv2.jsonapi.service.testutil;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;

import java.util.EnumMap;

public class DocumentUpdaterUtils {
  public static UpdateClause updateClause(UpdateOperator oper, ObjectNode args) {
    EnumMap<UpdateOperator, ObjectNode> operMap = new EnumMap<>(UpdateOperator.class);
    operMap.put(oper, args);
    return new UpdateClause(operMap);
  }

  public static UpdateClause updateClause(
      UpdateOperator oper1, ObjectNode args1, UpdateOperator oper2, ObjectNode args2) {
    EnumMap<UpdateOperator, ObjectNode> operMap = new EnumMap<>(UpdateOperator.class);
    operMap.put(oper1, args1);
    operMap.put(oper2, args2);
    return new UpdateClause(operMap);
  }
}
