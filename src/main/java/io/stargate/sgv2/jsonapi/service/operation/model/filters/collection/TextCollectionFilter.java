package io.stargate.sgv2.jsonapi.service.operation.model.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Optional;

/** Filters db documents based on a text field value */
public class TextCollectionFilter extends MapCollectionFilter<String> {
  private final String strValue;

  public TextCollectionFilter(String path, Operator operator, String value) {
    super("query_text_values", path, operator, value);
    this.strValue = value;
    if (Operator.EQ == operator || Operator.NE == operator)
      collectionIndexUsage.arrayContainsTag = true;
    else collectionIndexUsage.textIndexTag = true;
  }

  /**
   * Only update if the operation is eq so we know the value
   *
   * @param nodeFactory
   * @return
   */
  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    if (Operator.EQ.equals(operator)) {
      return Optional.of(toJsonNode(nodeFactory, strValue));
    }
    return Optional.empty();
  }

  //  @Override
  //  public JsonNode asJson(JsonNodeFactory nodeFactory) {
  //    return nodeFactory.textNode(strValue);
  //  }
  //
  //  @Override
  //  public boolean canAddField() {
  //    return Operator.EQ.equals(operator);
  //  }
}
