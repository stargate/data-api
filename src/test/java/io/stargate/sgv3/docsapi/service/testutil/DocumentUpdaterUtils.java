package io.stargate.sgv3.docsapi.service.testutil;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperator;
import java.util.EnumMap;

public class DocumentUpdaterUtils {
  public static UpdateClause updateClause(UpdateOperator oper, ObjectNode args) {
    EnumMap<UpdateOperator, ObjectNode> operMap = new EnumMap<>(UpdateOperator.class);
    operMap.put(oper, args);
    return new UpdateClause(operMap);
  }
}
