package io.stargate.sgv3.docsapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;

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
      throw new DocsException(
          ErrorCode.UNSUPPORTED_UPDATE_DATA_TYPE,
          "Unsupported update data type for UpdateClause (must be JSON Object): "
              + filterNode.getNodeType());
    }
    final EnumMap<UpdateOperator, ObjectNode> updateDefs = new EnumMap<>(UpdateOperator.class);
    Iterator<Map.Entry<String, JsonNode>> fieldIter = filterNode.fields();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      final String operName = entry.getKey();
      if (!operName.startsWith("$")) {
        throw new DocsException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION,
            "Invalid update operator '%s' (must start with '$')".formatted(operName));
      }
      UpdateOperator oper = UpdateOperator.getUpdateOperator(operName);
      if (oper == null) {
        throw new DocsException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION,
            "Unrecognized update operator '%s'".formatted(operName));
      }
      JsonNode operationArg = entry.getValue();
      if (!operationArg.isObject()) {
        throw new DocsException(
            ErrorCode.UNSUPPORTED_UPDATE_DATA_TYPE,
            "Unsupported update data type for Operator '%s' (must be JSON Object): %s"
                .formatted(operName, operationArg.getNodeType()));
      }
      updateDefs.put(oper, (ObjectNode) operationArg);
    }
    return new UpdateClause(updateDefs);
  }
}
