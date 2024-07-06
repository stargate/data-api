package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Optional;

/**
 * Filter for document where a field exists or not
 *
 * <p>NOTE: cannot do != null until we get NOT CONTAINS in the DB for set ?????TODO
 */
public class ExistsCollectionFilter extends SetCollectionFilter<String> {
  public ExistsCollectionFilter(String path, boolean existFlag) {
    super("exist_keys", path, path, existFlag ? Operator.CONTAINS : Operator.NOT_CONTAINS);
    this.indexUsage.existKeysIndexTag = true;
  }

  /**
   * DO not update the new document from an upsert for the $exists operation, we dont know the value
   * of the field
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
