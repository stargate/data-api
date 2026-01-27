package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.UpdateException;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
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
      throw UpdateException.Code.UNSUPPORTED_UPDATE_DATA_TYPE.get(
          Map.of(
              "errorMessage",
              "update data type for UpdateClause must be JSON Object, was: %s"
                  .formatted(JsonUtil.nodeTypeAsString(filterNode))));
    }
    final EnumMap<UpdateOperator, ObjectNode> updateDefs = new EnumMap<>(UpdateOperator.class);
    for (Map.Entry<String, JsonNode> entry : filterNode.properties()) {
      final String operName = entry.getKey();
      if (!operName.startsWith("$")) {
        throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION.get(
            Map.of(
                "errorMessage",
                "Invalid update operator '%s' (must start with '$')".formatted(operName)));
      }
      UpdateOperator oper = UpdateOperator.getUpdateOperator(operName);
      if (oper == null) {
        throw UpdateException.Code.UNSUPPORTED_UPDATE_OPERATION.get(
            Map.of("errorMessage", "Unrecognized update operator '%s'".formatted(operName)));
      }
      JsonNode operationArg = entry.getValue();
      if (!operationArg.isObject()) {
        throw UpdateException.Code.UNSUPPORTED_UPDATE_DATA_TYPE.get(
            Map.of(
                "errorMessage",
                "update data type for Operator '%s' must be JSON Object, was: %s"
                    .formatted(operName, JsonUtil.nodeTypeAsString(operationArg))));
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
          throw SchemaException.Code.INVALID_USAGE_OF_VECTORIZE.get(Map.of("extraDesc", ""));
        }
      }
    }
  }
}
