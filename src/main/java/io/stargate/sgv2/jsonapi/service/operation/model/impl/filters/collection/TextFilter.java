package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/** Filters db documents based on a text field value */
public class TextFilter extends MapFilterBase<String> {
  private final String strValue;

  public TextFilter(String path, Operator operator, String value) {
    super("query_text_values", path, operator, value);
    this.strValue = value;
    if (Operator.EQ == operator || Operator.NE == operator) indexUsage.arrayContainsTag = true;
    else indexUsage.textIndexTag = true;
  }

  @Override
  public JsonNode asJson(JsonNodeFactory nodeFactory) {
    return nodeFactory.textNode(strValue);
  }

  @Override
  public boolean canAddField() {
    return Operator.EQ.equals(operator);
  }
}
