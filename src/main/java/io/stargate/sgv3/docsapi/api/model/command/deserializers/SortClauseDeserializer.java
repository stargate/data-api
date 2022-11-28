package io.stargate.sgv3.docsapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv3.docsapi.api.model.command.clause.sort.SortExpression;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    JsonNode node = parser.getCodec().readTree(parser);

    // if missing or null, return null back
    if (node.isMissingNode() || node.isNull()) {
      return null;
    }

    // TODO, should we have specific exceptions, or throw generic JSON ones?

    // otherwise, if it's not array throw exception
    if (!node.isArray()) {
      throw new JsonMappingException(
          parser, "Sort clause must be submitted as the array of strings.");
    }

    // safe iterate, we know it's array
    List<SortExpression> expressions = new ArrayList<>();
    for (JsonNode inner : node) {
      if (!inner.isTextual()) {
        throw new JsonMappingException(
            parser, "Sort clause expression must be represented as strings.");
      }

      String text = inner.asText();
      if (text.isBlank()) {
        throw new JsonMappingException(
            parser, "Sort clause expression must be represented as not-blank strings.");
      }

      boolean ascending = text.charAt(0) != '-';
      String path = ascending ? text : text.substring(1);
      if (path.isBlank()) {
        throw new JsonMappingException(parser, "A sort clause expression path must not be blank.");
      }

      SortExpression exp = new SortExpression(path.trim(), ascending);
      expressions.add(exp);
    }

    return new SortClause(expressions);
  }
}
