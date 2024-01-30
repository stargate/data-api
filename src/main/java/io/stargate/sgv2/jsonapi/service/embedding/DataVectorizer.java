package io.stargate.sgv2.jsonapi.service.embedding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
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
  public Uni<Boolean> vectorize(List<JsonNode> documents) {
    int vectorDataPosition = 0;
    List<String> vectorizeTexts = new ArrayList<>();
    Map<Integer, Integer> vectorizeMap = new HashMap<>();
    for (int position = 0; position < documents.size(); position++) {
      JsonNode document = documents.get(position);
      if (document.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
        if (document.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
          throw new JsonApiException(ErrorCode.INVALID_USAGE_OF_VECTORIZE);
        }
        final JsonNode jsonNode =
            document.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
        if (jsonNode.isNull()) {
          ((ObjectNode) document)
              .put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
          continue;
        }
        if (!jsonNode.isTextual()) {
          throw new JsonApiException(ErrorCode.SHRED_BAD_VECTORIZE_VALUE);
        }

        vectorizeTexts.add(jsonNode.asText());
        vectorizeMap.put(vectorDataPosition, position);
        vectorDataPosition++;
      }
    }

    if (!vectorizeTexts.isEmpty()) {
      if (embeddingService == null) {
        throw new JsonApiException(ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE);
      }
      Uni<List<float[]>> vectors = embeddingService.vectorize(vectorizeTexts);
      return vectors
          .onItem()
          .transform(
              vectorData -> {
                for (int vectorPosition = 0; vectorPosition < vectorData.size(); vectorPosition++) {
                  int position = vectorizeMap.get(vectorPosition);
                  JsonNode document = documents.get(position);
                  float[] vector = vectorData.get(vectorPosition);
                  final ArrayNode arrayNode = nodeFactory.arrayNode(vector.length);
                  for (float listValue : vector) {
                    arrayNode.add(nodeFactory.numberNode(listValue));
                  }
                  ((ObjectNode) document)
                      .put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, arrayNode);
                }
                return true;
              });
    }
    return Uni.createFrom().item(true);
  }

  /**
   * Vectorize the '$vectorize' fields in the sort clause
   *
   * @param sortClause - Sort clause to be vectorized
   */
  public Uni<Boolean> vectorize(SortClause sortClause) {
    if (sortClause == null || sortClause.sortExpressions().isEmpty())
      return Uni.createFrom().item(true);
    if (sortClause.hasVectorizeSearchClause()) {
      final List<SortExpression> sortExpressions = sortClause.sortExpressions();
      SortExpression expression = sortExpressions.get(0);
      String text = expression.vectorize();
      if (embeddingService == null) {
        throw new JsonApiException(ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE);
      }
      Uni<List<float[]>> vectors = embeddingService.vectorize(List.of(text));
      vectors
          .onItem()
          .transform(
              vectorData -> {
                sortExpressions.clear();
                sortExpressions.add(SortExpression.vsearch(vectorData.get(0)));
                return true;
              });
    }
    return Uni.createFrom().item(true);
  }

  /**
   * Vectorize the '$vectorize' fields in the update clause
   *
   * @param updateClause - Update clause to be vectorized
   */
  public Uni<Boolean> vectorizeUpdateClause(UpdateClause updateClause) {
    if (updateClause == null) return Uni.createFrom().item(true);
    final ObjectNode setNode = updateClause.updateOperationDefs().get(UpdateOperator.SET);
    final ObjectNode setOnInsertNode =
        updateClause.updateOperationDefs().get(UpdateOperator.SET_ON_INSERT);
    return updateVectorize(setNode)
        .onItem()
        .transform(
            vectorized -> {
              return updateVectorize(setOnInsertNode)
                  .onItem()
                  .transform(
                      v -> {
                        return true;
                      });
            })
        .onItem()
        .transform(
            v -> {
              final ObjectNode unsetNode =
                  updateClause.updateOperationDefs().get(UpdateOperator.UNSET);
              if (unsetNode != null
                  && unsetNode.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
                if (unsetNode.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
                  throw new JsonApiException(ErrorCode.INVALID_USAGE_OF_VECTORIZE);
                }
                unsetNode.putNull(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
              }
              return true;
            });
  }

  private Uni<Boolean> updateVectorize(ObjectNode node) {
    if (node == null) return Uni.createFrom().item(true);
    if (node.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
      if (node.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
        throw new JsonApiException(ErrorCode.INVALID_USAGE_OF_VECTORIZE);
      }
      final JsonNode jsonNode = node.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
      if (jsonNode.isNull()) {
        node.putNull(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
      } else if (jsonNode.isTextual()) {
        final String text = jsonNode.asText();
        if (embeddingService == null) {
          throw new JsonApiException(ErrorCode.UNAVAILABLE_EMBEDDING_SERVICE);
        }
        final Uni<List<float[]>> vectors = embeddingService.vectorize(List.of(text));
        return vectors
            .onItem()
            .transform(
                vectorData -> {
                  final ArrayNode arrayNode = nodeFactory.arrayNode(vectorData.get(0).length);
                  for (float listValue : vectorData.get(0)) {
                    arrayNode.add(nodeFactory.numberNode(listValue));
                  }
                  node.put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, arrayNode);
                  return true;
                });
      } else {
        throw new JsonApiException(
            ErrorCode.SHRED_BAD_VECTORIZE_VALUE, ErrorCode.SHRED_BAD_VECTORIZE_VALUE.getMessage());
      }
    }
    return Uni.createFrom().item(true);
  }
}
