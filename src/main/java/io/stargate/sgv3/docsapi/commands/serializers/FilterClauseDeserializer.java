package io.stargate.sgv3.docsapi.commands.serializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

// Aaron - these should be replaced with the approach used elsewherre, see {@link CommandSerializer}
public class FilterClauseDeserializer extends StdDeserializer<FilterClause> {

  public FilterClauseDeserializer() {
    this(null);
  }

  public FilterClauseDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public FilterClause deserialize(JsonParser parser, DeserializationContext ctxt)
      throws IOException, JacksonException {

    var filter = new FilterClause();
    JsonNode filterNode = parser.getCodec().readTree(parser);

    var fieldIter = filterNode.fields();
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> entry = fieldIter.next();

      // TODO: Does not handle logical expressions, they are out of scope
      // HACK can only handle literal values...
      var operatorExpression = entry.getValue();
      assert operatorExpression.isValueNode();

      filter.eq(entry.getKey(), jsonNodeValue(entry.getValue()));
    }

    return filter;
  }

  private static Object jsonNodeValue(JsonNode node) {
    switch (node.getNodeType()) {
      case BOOLEAN:
        return node.asBoolean();
      case NUMBER:
        return new BigDecimal(node.asDouble());
      case STRING:
        return node.asText();
      case NULL:
        return null;
      default:
        throw new RuntimeException(String.format("Unsupported NodeType %s", node.getNodeType()));
    }
  }
}
