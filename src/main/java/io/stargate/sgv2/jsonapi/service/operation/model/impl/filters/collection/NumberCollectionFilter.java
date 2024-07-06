package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.math.BigDecimal;
import java.util.Optional;

/** Filters db documents based on a numeric field value */
public class NumberCollectionFilter extends MapCollectionFilter<BigDecimal> {
  private final BigDecimal numberValue;

  public NumberCollectionFilter(String path, Operator operator, BigDecimal value) {
    super("query_dbl_values", path, operator, value);
    this.numberValue = value;
    if (Operator.EQ == operator || Operator.NE == operator) indexUsage.arrayContainsTag = true;
    else indexUsage.numberIndexTag = true;
  }

  /**
   * Only update if the operation is eq
   *
   * @param nodeFactory
   * @return
   */
  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    if (Operator.EQ.equals(operator)) {
      return Optional.of(toJsonNode(nodeFactory, numberValue));
    }
    return Optional.empty();
  }

  //    @Override
  //    public JsonNode asJson(JsonNodeFactory nodeFactory) {
  //        return nodeFactory.numberNode(numberValue);
  //    }
  //
  //    @Override
  //    public boolean canAddField() {
  //        return Operator.EQ.equals(operator);
  //    }
}
