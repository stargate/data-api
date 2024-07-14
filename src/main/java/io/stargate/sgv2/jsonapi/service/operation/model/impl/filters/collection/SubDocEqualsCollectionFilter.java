package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import java.util.Map;
import java.util.Optional;

/**
 * Filter for document where field is subdocument and matches (same subfield in same order) the
 * filter sub document
 */
public class SubDocEqualsCollectionFilter extends MapCollectionFilter<String> {
  private final Map<String, Object> subDocValue;

  public SubDocEqualsCollectionFilter(
      DocValueHasher hasher, String path, Map<String, Object> subDocData, Operator operator) {
    super("query_text_values", path, operator, getHash(hasher, subDocData));
    this.indexUsage.textIndexTag = true;
    this.subDocValue = subDocData;
  }

  /**
   * We know the new doc should have the same values as the filter
   *
   * @param nodeFactory
   * @return
   */
  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    return Optional.of(toJsonNode(nodeFactory, subDocValue));
  }

  //    @Override
  //    public JsonNode asJson(JsonNodeFactory nodeFactory) {
  //        return DBFilterBase.getJsonNode(nodeFactory, subDocValue);
  //    }
  //
  //    @Override
  //    public boolean canAddField() {
  //        return true;
  //    }
}
