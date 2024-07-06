package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;

/** Filters db documents based on a date field value */
public class DateCollectionFilter extends MapCollectionFilter<Instant> {
  private final Date dateValue;

  public DateCollectionFilter(String path, Operator operator, Date value) {
    super("query_timestamp_values", path, operator, Instant.ofEpochMilli(value.getTime()));
    this.dateValue = value;
    if (Operator.EQ == operator || Operator.NE == operator) indexUsage.arrayContainsTag = true;
    else indexUsage.timestampIndexTag = true;
  }

  /**
   * Only update the new document from an upsert for this array operation if the operator is EQ
   *
   * @param nodeFactory
   * @return
   */
  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    if (Operator.EQ.equals(operator)) {
      return Optional.of(toJsonNode(nodeFactory, dateValue));
    }
    return Optional.empty();
  }

  //    @Override
  //    public JsonNode asJson(JsonNodeFactory nodeFactory) {
  //        return nodeFactory.numberNode(dateValue.getTime());
  //    }
  //
  //    @Override
  //    public boolean canAddField() {
  //        return Operator.EQ.equals(operator);
  //    }
}
