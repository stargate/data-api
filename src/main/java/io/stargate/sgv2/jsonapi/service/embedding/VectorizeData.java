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

public class VectorizeData {
  private final EmbeddingService embeddingService;
  private final JsonNodeFactory nodeFactory;

  public VectorizeData(EmbeddingService embeddingService, JsonNodeFactory nodeFactory) {
    this.embeddingService = embeddingService;
    this.nodeFactory = nodeFactory;
  }

  public void vectorize(List<JsonNode> documents) {
    int vectorDataPosition = 0;
    List<String> vectorizeTexts = new ArrayList<>();
    Map<Integer, Integer> vectorizeMap = new HashMap<>();
    for (int position = 0; position < documents.size(); position++) {
      JsonNode document = documents.get(position);
      if (document.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
        final JsonNode jsonNode =
            document.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
        if (jsonNode.isNull()) {
          ((ObjectNode) document).remove(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
          ((ObjectNode) document).remove(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
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
        ((ObjectNode) document).remove(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
      }
    }
  }

  public void vectorize(SortClause sortClause) {
    if (sortClause == null || sortClause.sortExpressions().isEmpty()) return;
    if (sortClause.hasVectorizeSearchClause()) {
      final List<SortExpression> sortExpressions = sortClause.sortExpressions();
      SortExpression expression = sortExpressions.get(0);
      String text = expression.vectorize();
      List<float[]> vectors = embeddingService.vectorize(List.of(text));
      sortExpressions.clear();
      sortExpressions.add(SortExpression.vsearch(vectors.get(0)));
    }
  }

  public void vectorizeUpdateClause(UpdateClause updateClause) {
    final ObjectNode setNode = updateClause.updateOperationDefs().get(UpdateOperator.SET);
    final ObjectNode setOnInsertNode =
        updateClause.updateOperationDefs().get(UpdateOperator.SET_ON_INSERT);
    updateVectorize(setNode);
    updateVectorize(setOnInsertNode);
    final ObjectNode unsetNode = updateClause.updateOperationDefs().get(UpdateOperator.UNSET);
    if (unsetNode != null && unsetNode.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
      unsetNode.remove(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
      unsetNode.putNull(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
    }
  }

  private void updateVectorize(ObjectNode node) {
    if (node == null) return;
    if (node.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
      final JsonNode jsonNode = node.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
      if (jsonNode.isNull()) {
        node.remove(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
        node.putNull(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
      } else if (jsonNode.isTextual()) {
        final String text = jsonNode.asText();
        final List<float[]> vectors = embeddingService.vectorize(List.of(text));
        final ArrayNode arrayNode = nodeFactory.arrayNode(vectors.get(0).length);
        for (float listValue : vectors.get(0)) {
          arrayNode.add(nodeFactory.numberNode(listValue));
        }
        node.put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, arrayNode);
        node.remove(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
      } else {
        throw new JsonApiException(
            ErrorCode.SHRED_BAD_VECTORIZE_VALUE, ErrorCode.SHRED_BAD_VECTORIZE_VALUE.getMessage());
      }
    }
  }
}
