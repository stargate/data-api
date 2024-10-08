package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.io.IOException;
import java.util.*;

/** {@link StdDeserializer} for the {@link UpdateClause}. */
public class UpdateClauseDeserializer extends StdDeserializer<UpdateClause> {

  public UpdateClauseDeserializer() {
    super(UpdateClause.class);
  }

  /** {@inheritDoc} */
  @Override
  public UpdateClause deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonNode filterNode = deserializationContext.readTree(jsonParser);
    if (!filterNode.isObject()) {
      throw ErrorCodeV1.UNSUPPORTED_UPDATE_DATA_TYPE.toApiException(
          "update data type for UpdateClause must be JSON Object, was: %s",
          filterNode.getNodeType());
    }
    final EnumMap<UpdateOperator, ObjectNode> updateDefs = new EnumMap<>(UpdateOperator.class);
    Iterator<Map.Entry<String, JsonNode>> fieldIter = filterNode.fields();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      final String operName = entry.getKey();
      if (!operName.startsWith("$")) {
        throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION.toApiException(
            "Invalid update operator '%s' (must start with '$')", operName);
      }
      UpdateOperator oper = UpdateOperator.getUpdateOperator(operName);
      if (oper == null) {
        throw ErrorCodeV1.UNSUPPORTED_UPDATE_OPERATION.toApiException(
            "Unrecognized update operator '%s'", operName);
      }
      JsonNode operationArg = entry.getValue();
      if (!operationArg.isObject()) {
        throw ErrorCodeV1.UNSUPPORTED_UPDATE_DATA_TYPE.toApiException(
            "update data type for Operator '%s' must be JSON Object, was: %s",
            operName, operationArg.getNodeType());
      }
      updateDefs.put(oper, (ObjectNode) operationArg);
    }
    validateUpdateDefs(updateDefs);
    return new UpdateClause(updateDefs);
  }

  public void validateUpdateDefs(EnumMap<UpdateOperator, ObjectNode> updateDefs) {
    // check1: can not unset $vectorize and $vector at the same time
    List<ObjectNode> checkUpdateOperationNodes = new ArrayList<>();
    checkUpdateOperationNodes.add(updateDefs.get(UpdateOperator.UNSET));
    checkUpdateOperationNodes.add(updateDefs.get(UpdateOperator.SET));
    for (ObjectNode checkUpdateOperationNode : checkUpdateOperationNodes) {
      if (checkUpdateOperationNode != null
          && checkUpdateOperationNode.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
        if (checkUpdateOperationNode.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
          throw ErrorCodeV1.INVALID_USAGE_OF_VECTORIZE.toApiException();
        }
      }
    }
  }
}
