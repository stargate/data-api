package io.stargate.sgv3.docsapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** {@link StdDeserializer} for the {@link FilterClause}. */
public class FilterClauseDeserializer extends StdDeserializer<FilterClause> {

  public FilterClauseDeserializer() {
    super(FilterClause.class);
  }

  /** {@inheritDoc} */
  @Override
  public FilterClause deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JacksonException {
    JsonNode filterNode = deserializationContext.readTree(jsonParser);
    Iterator<Map.Entry<String, JsonNode>> fieldIter = filterNode.fields();
    List<ComparisonExpression> expressionList = new ArrayList<>();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();
      // TODO: Does not handle logical expressions, they are out of scope
      JsonNode operatorExpression = entry.getValue();
      assert operatorExpression.isValueNode();
      expressionList.add(ComparisonExpression.eq(entry.getKey(), jsonNodeValue(entry.getValue())));
    }
    return new FilterClause(expressionList);
  }

  private static Object jsonNodeValue(JsonNode node) {
    switch (node.getNodeType()) {
      case BOOLEAN:
        return node.booleanValue();
      case NUMBER:
        return node.decimalValue();
      case STRING:
        return node.textValue();
      case NULL:
        return null;
      default:
        throw new DocsException(
            ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
            String.format("Unsupported NodeType %s", node.getNodeType()));
    }
  }
}
