package io.stargate.sgv2.jsonapi.service.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to execute embedding serive to get vector embeddings for the text fields in the
 * '$vectorize' field. The class has three utility methods to handle vectorization in json
 * documents, sort clause and update clause.
 */
public class DataVectorizer {
  private final EmbeddingService embeddingService;
  private final JsonNodeFactory nodeFactory;

  /**
   * Constructor
   *
   * @param embeddingService - Service client based on embedding service configuration set for the
   *     table
   * @param nodeFactory - Jackson node factory to create json nodes added to the document
   */
  public DataVectorizer(EmbeddingService embeddingService, JsonNodeFactory nodeFactory) {
    this.embeddingService = embeddingService;
    this.nodeFactory = nodeFactory;
  }

  /**
   * Vectorize the '$vectorize' fields in the document
   *
   * @param documents - Documents to be vectorized
   */
  public void vectorize(List<JsonNode> documents) {
    int vectorDataPosition = 0;
    List<String> vectorizeTexts = new ArrayList<>();
    Map<Integer, Integer> vectorizeMap = new HashMap<>();
    for (int position = 0; position < documents.size(); position++) {
      JsonNode document = documents.get(position);
      if (document.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
        if (document.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
          throw new JsonApiException(
              ErrorCode.INVALID_USAGE_OF_VECTORIZE,
              ErrorCode.INVALID_USAGE_OF_VECTORIZE.getMessage());
        }
        final JsonNode jsonNode =
            document.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
        if (jsonNode.isNull()) {
          ((ObjectNode) document)
              .put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
          continue;
        }
        if (!jsonNode.isTextual()) {
          throw new JsonApiException(
              ErrorCode.SHRED_BAD_VECTORIZE_VALUE,
              ErrorCode.SHRED_BAD_VECTORIZE_VALUE.getMessage());
        }

        vectorizeTexts.add(jsonNode.asText());
        vectorizeMap.put(vectorDataPosition, position);
        vectorDataPosition++;
      }
    }

    if (!vectorizeTexts.isEmpty()) {
      if (embeddingService == null) {
        throw new JsonApiException(
            ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE,
            ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE.getMessage());
      }
      List<float[]> vectors = embeddingService.vectorize(vectorizeTexts);
      for (int vectorPosition = 0; vectorPosition < vectors.size(); vectorPosition++) {
        int position = vectorizeMap.get(vectorPosition);
        JsonNode document = documents.get(position);
        float[] vector = vectors.get(vectorPosition);
        final ArrayNode arrayNode = nodeFactory.arrayNode(vector.length);
        for (float listValue : vector) {
          arrayNode.add(nodeFactory.numberNode(listValue));
        }
        ((ObjectNode) document).put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, arrayNode);
      }
    }
  }

  /**
   * Vectorize the '$vectorize' fields in the sort clause
   *
   * @param sortClause - Sort clause to be vectorized
   */
  public void vectorize(SortClause sortClause) {
    if (sortClause == null || sortClause.sortExpressions().isEmpty()) return;
    if (sortClause.hasVectorizeSearchClause()) {
      final List<SortExpression> sortExpressions = sortClause.sortExpressions();
      SortExpression expression = sortExpressions.get(0);
      String text = expression.vectorize();
      if (embeddingService == null) {
        throw new JsonApiException(
            ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE,
            ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE.getMessage());
      }
      List<float[]> vectors = embeddingService.vectorize(List.of(text));
      sortExpressions.clear();
      sortExpressions.add(SortExpression.vsearch(vectors.get(0)));
    }
  }

  /**
   * Vectorize the '$vectorize' fields in the update clause
   *
   * @param updateClause - Update clause to be vectorized
   */
  public void vectorizeUpdateClause(UpdateClause updateClause) {
    if (updateClause == null) return;
    final ObjectNode setNode = updateClause.updateOperationDefs().get(UpdateOperator.SET);
    final ObjectNode setOnInsertNode =
        updateClause.updateOperationDefs().get(UpdateOperator.SET_ON_INSERT);
    updateVectorize(setNode);
    updateVectorize(setOnInsertNode);
    final ObjectNode unsetNode = updateClause.updateOperationDefs().get(UpdateOperator.UNSET);
    if (unsetNode != null && unsetNode.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
      if (unsetNode.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
        throw new JsonApiException(
            ErrorCode.INVALID_USAGE_OF_VECTORIZE,
            ErrorCode.INVALID_USAGE_OF_VECTORIZE.getMessage());
      }
      unsetNode.putNull(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
    }
  }

  private void updateVectorize(ObjectNode node) {
    if (node == null) return;
    if (node.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
      if (node.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
        throw new JsonApiException(
            ErrorCode.INVALID_USAGE_OF_VECTORIZE,
            ErrorCode.INVALID_USAGE_OF_VECTORIZE.getMessage());
      }
      final JsonNode jsonNode = node.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
      if (jsonNode.isNull()) {
        node.putNull(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
      } else if (jsonNode.isTextual()) {
        final String text = jsonNode.asText();
        if (embeddingService == null) {
          throw new JsonApiException(
              ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE,
              ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE.getMessage());
        }
        final List<float[]> vectors = embeddingService.vectorize(List.of(text));
        final ArrayNode arrayNode = nodeFactory.arrayNode(vectors.get(0).length);
        for (float listValue : vectors.get(0)) {
          arrayNode.add(nodeFactory.numberNode(listValue));
        }
        node.put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, arrayNode);
      } else {
        throw new JsonApiException(
            ErrorCode.SHRED_BAD_VECTORIZE_VALUE, ErrorCode.SHRED_BAD_VECTORIZE_VALUE.getMessage());
      }
    }
  }
}
