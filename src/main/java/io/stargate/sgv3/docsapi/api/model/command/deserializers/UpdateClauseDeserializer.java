package io.stargate.sgv3.docsapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation.UpdateValue;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperation.UpdateValue.ValueType;
import io.stargate.sgv3.docsapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** {@link StdDeserializer} for the {@link UpdateClause}. */
public class UpdateClauseDeserializer extends StdDeserializer<UpdateClause> {

  public UpdateClauseDeserializer() {
    super(UpdateClause.class);
  }

  /** {@inheritDoc} */
  @Override
  public UpdateClause deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode filterNode = deserializationContext.readTree(jsonParser);
    if (!filterNode.isObject()) throw new DocsException(ErrorCode.UNSUPPORTED_UPDATE_DATA_TYPE);
    Iterator<Map.Entry<String, JsonNode>> fieldIter = filterNode.fields();
    List<UpdateOperation<?>> expressionList = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      if (entry.getKey().startsWith("$")) {
        // Using aggregation pipeline
        getOperations(entry, expressionList);
      } else {
        // default is set
        JsonNode value = entry.getValue();
        if (!value.isValueNode()) throw new DocsException(ErrorCode.UNSUPPORTED_UPDATE_DATA_TYPE);
        expressionList.add(
            new UpdateOperation(entry.getKey(), UpdateOperator.SET, jsonNodeValue(value)));
      }
    }
    return new UpdateClause(expressionList);
  }

  /**
   * The key of the entry will be update type like $set, $unset The value of the entry will be
   * object of field to be updated "$set" : {"field1" : val1, "field2" : val2 }
   *
   * @param entry
   * @return
   */
  private void getOperations(
      Map.Entry<String, JsonNode> entry, List<UpdateOperation<?>> expressionList) {
    try {
      UpdateOperator operator = UpdateOperator.getUpdateOperator(entry.getKey());
      if (entry.getValue().isObject()) {
        final Iterator<Map.Entry<String, JsonNode>> fields = entry.getValue().fields();
        while (fields.hasNext()) {
          Map.Entry<String, JsonNode> updateField = fields.next();
          JsonNode value = updateField.getValue();
          if (!value.isValueNode()) throw new DocsException(ErrorCode.UNSUPPORTED_UPDATE_DATA_TYPE);
          expressionList.add(
              new UpdateOperation(updateField.getKey(), operator, jsonNodeValue(value)));
        }
      } else {
        throw new DocsException(ErrorCode.UNSUPPORTED_UPDATE_DATA_TYPE);
      }
    } catch (IllegalArgumentException e) {
      throw new DocsException(
          ErrorCode.UNSUPPORTED_UPDATE_OPERATION, "Unsupported update operation " + entry.getKey());
    }
  }

  private static UpdateValue jsonNodeValue(JsonNode node) {
    switch (node.getNodeType()) {
      case BOOLEAN:
        return new UpdateValue<>(node.booleanValue(), ValueType.BOOLEAN);
      case NUMBER:
        return new UpdateValue<>(node.decimalValue(), ValueType.NUMBER);
      case STRING:
        return new UpdateValue<>(node.textValue(), ValueType.STRING);
      case NULL:
        return new UpdateValue<String>(null, ValueType.NULL);
      default:
        throw new DocsException(
            ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
            String.format("Unsupported NodeType %s", node.getNodeType()));
    }
  }
}
