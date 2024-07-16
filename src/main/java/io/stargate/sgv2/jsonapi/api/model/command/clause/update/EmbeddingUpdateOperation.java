package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;

public record EmbeddingUpdateOperation(String vectorizeContent) {
  //        UpdateOperation.UpdateOperationResult<? extends ActionWithLocator>
  // updateDocument(ObjectNode doc, float[] vector)

  /**
   * // TODO 一定会换的，因为我们已经知道 vectorize 有 diff了 , 而且你已经拿到vector array了
   *
   * <p>update the document with corresponding vector
   *
   * @param doc Document to update
   * @param dataVectorizer dataVectorizer
   * @return Uni<Boolean> modified
   */
  public void updateDocument(JsonNode doc, float[] vector) {
    // TODO can I do this instancitation?
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNodeFactory nodeFactory = objectMapper.getNodeFactory();
    final JsonNode vectorJsonNode = doc.get(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
    final ArrayNode arrayNode = nodeFactory.arrayNode(vector.length);
    for (float listValue : vector) {
      arrayNode.add(nodeFactory.numberNode(listValue));
    }
    ((ObjectNode) doc).put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, arrayNode);
  }
}
