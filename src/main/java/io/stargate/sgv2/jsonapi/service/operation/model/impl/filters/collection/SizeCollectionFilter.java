package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Optional;

/** Filter for document where array has specified number of elements */
public class SizeCollectionFilter extends MapCollectionFilter<Integer> {
  public SizeCollectionFilter(String path, Operator operator, Integer size) {
    super("array_size", path, operator, size);
    this.indexUsage.arraySizeIndexTag = true;
  }

  /**
   * Do not update, we only know the size not what is meant to be in the array
   *
   * @param nodeFactory
   * @return
   */
  @Override
  protected Optional<JsonNode> jsonNodeForNewDocument(JsonNodeFactory nodeFactory) {
    return Optional.empty();
  }

  //    @Override
  //    public JsonNode asJson(JsonNodeFactory nodeFactory) {
  //        return null;
  //    }
  //
  //    @Override
  //    public boolean canAddField() {
  //        return false;
  //    }
}
