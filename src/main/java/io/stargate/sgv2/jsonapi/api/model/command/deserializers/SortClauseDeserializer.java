package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** {@link StdDeserializer} for the {@link SortClause}. */
public class SortClauseDeserializer extends StdDeserializer<SortClause> {

  /** No-arg constructor explicitly needed. */
  public SortClauseDeserializer() {
    super(SortClause.class);
  }

  /** {@inheritDoc} */
  @Override
  public SortClause deserialize(JsonParser parser, DeserializationContext ctxt)
      throws IOException, JacksonException {
    JsonNode node = ctxt.readTree(parser);

    // if missing or null, return null back
    if (node.isMissingNode() || node.isNull()) {
      // TODO should we return empty sort clause instead?
      return null;
    }

    // TODO, should we have specific exceptions, or throw generic JSON ones?
    //  https://github.com/riptano/sgv3-docsapi/issues/9

    // otherwise, if it's not object throw exception
    if (!node.isObject()) {
      throw new JsonMappingException(parser, "Sort clause must be submitted as json object");
    }

    ObjectNode sortNode = (ObjectNode) node;

    // safe iterate, we know it's array
    Iterator<Map.Entry<String, JsonNode>> fieldIter = sortNode.fields();
    List<SortExpression> sortExpressions = new ArrayList<>(sortNode.size());
    while (fieldIter.hasNext()) {
      Map.Entry<String, JsonNode> inner = fieldIter.next();

      if (!inner.getValue().isInt()
          || !(inner.getValue().asInt() == 1 || inner.getValue().asInt() == -1)) {
        throw new JsonMappingException(
            parser, "Sort ordering value can only be `1` for ascending or `-1` for descending.");
      }

      String path = inner.getKey().trim();
      if (path.isBlank()) {
        throw new JsonMappingException(
            parser, "Sort clause expression must be represented as not-blank strings.");
      }

      boolean ascending = inner.getValue().asInt() == 1;

      SortExpression exp = new SortExpression(path, ascending);
      sortExpressions.add(exp);
    }

    return new SortClause(sortExpressions);
  }
}
