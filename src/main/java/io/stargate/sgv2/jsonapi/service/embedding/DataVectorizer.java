package io.stargate.sgv2.jsonapi.service.embedding;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE;

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
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
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
  private final EmbeddingProvider embeddingProvider;
  private final JsonNodeFactory nodeFactory;
  private final String embeddingApiKey;
  private final SchemaObject schemaObject;

  /**
   * Constructor
   *
   * @param embeddingProvider - Service client based on embedding service configuration set for the
   *     table
   * @param nodeFactory - Jackson node factory to create json nodes added to the document
   * @param embeddingApiKey - Optional override embedding api key came in request header
   * @param schemaObject - The collection setting for vectorize call
   */
  public DataVectorizer(
      EmbeddingProvider embeddingProvider,
      JsonNodeFactory nodeFactory,
      String embeddingApiKey,
    SchemaObject schemaObject) {

    this.embeddingProvider = embeddingProvider;
    this.nodeFactory = nodeFactory;
    this.embeddingApiKey = embeddingApiKey;
    this.schemaObject = schemaObject;
  }

  /**
   * Vectorize the '$vectorize' fields in the document
   *
   * @param documents - Documents to be vectorized
   */
  public Uni<Boolean> vectorize(List<JsonNode> documents) {
    try {
      int vectorDataPosition = 0;
      List<String> vectorizeTexts = new ArrayList<>();
      Map<Integer, Integer> vectorizeMap = new HashMap<>();
      for (int position = 0; position < documents.size(); position++) {
        JsonNode document = documents.get(position);
        if (document.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
          if (document.has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
            throw new JsonApiException(
                ErrorCode.INVALID_USAGE_OF_VECTORIZE,
                ErrorCode.INVALID_USAGE_OF_VECTORIZE.getMessage()
                    + ", issue in document at position "
                    + (position + 1));
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
                ErrorCode.INVALID_VECTORIZE_VALUE_TYPE,
                ErrorCode.INVALID_VECTORIZE_VALUE_TYPE.getMessage()
                    + ", issue in document at position "
                    + (position + 1));
          }

          String vectorizeData = jsonNode.asText();
          if (vectorizeData.isBlank()) {
            ((ObjectNode) document)
                .put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
            continue;
          }

          vectorizeTexts.add(vectorizeData);
          vectorizeMap.put(vectorDataPosition, position);
          vectorDataPosition++;
        }
      }

      if (!vectorizeTexts.isEmpty()) {
        if (embeddingProvider == null) {
          throw ErrorCode.EMBEDDING_SERVICE_NOT_CONFIGURED.toApiException(
              schemaObject.name.table());
        }
        Uni<List<float[]>> vectors =
            embeddingProvider
                .vectorize(
                    1,
                    vectorizeTexts,
                    embeddingApiKey,
                    EmbeddingProvider.EmbeddingRequestType.INDEX)
                .map(res -> res.embeddings());
        return vectors
            .onItem()
            .transform(
                vectorData -> {
                  // check if we get back the same number of vectors that we asked for
                  if (vectorData.size() != vectorizeTexts.size()) {
                    throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                        "Embedding provider '%s' didn't return the expected number of embeddings. Expect: '%d'. Actual: '%d'",
                        schemaObject.vectorConfig().vectorizeConfig().provider(),
                        vectorizeTexts.size(),
                        vectorData.size());
                  }
                  for (int vectorPosition = 0;
                      vectorPosition < vectorData.size();
                      vectorPosition++) {
                    int position = vectorizeMap.get(vectorPosition);
                    JsonNode document = documents.get(position);
                    float[] vector = vectorData.get(vectorPosition);
                    // check if all vectors have the expected size
                    if (vector.length != schemaObject.vectorConfig().vectorSize()) {
                      throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                          "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                          schemaObject.vectorConfig().vectorizeConfig().provider(),
                          schemaObject.vectorConfig().vectorSize(),
                          vector.length);
                    }
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
    } catch (JsonApiException e) {
      return Uni.createFrom().failure(e);
    }
  }

  /**
   * Vectorize the '$vectorize' fields in the sort clause
   *
   * @param sortClause - Sort clause to be vectorized
   */
  public Uni<Boolean> vectorize(SortClause sortClause) {
    try {
      if (sortClause == null || sortClause.sortExpressions().isEmpty())
        return Uni.createFrom().item(true);
      if (sortClause.hasVectorizeSearchClause()) {
        final List<SortExpression> sortExpressions = sortClause.sortExpressions();
        SortExpression expression = sortExpressions.get(0);
        String text = expression.vectorize();
        if (embeddingProvider == null) {
          throw ErrorCode.EMBEDDING_SERVICE_NOT_CONFIGURED.toApiException(
              schemaObject.name.table());
        }
        Uni<List<float[]>> vectors =
            embeddingProvider
                .vectorize(
                    1,
                    List.of(text),
                    embeddingApiKey,
                    EmbeddingProvider.EmbeddingRequestType.SEARCH)
                .map(res -> res.embeddings());
        return vectors
            .onItem()
            .transform(
                vectorData -> {
                  float[] vector = vectorData.get(0);
                  // check if vector have the expected size
                  if (vector.length != schemaObject.vectorConfig().vectorSize()) {
                    throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                        "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                        schemaObject.vectorConfig().vectorizeConfig().provider(),
                        schemaObject.vectorConfig().vectorSize(),
                        vector.length);
                  }
                  sortExpressions.clear();
                  sortExpressions.add(SortExpression.vsearch(vector));
                  return true;
                });
      }
      return Uni.createFrom().item(true);
    } catch (JsonApiException e) {
      return Uni.createFrom().failure(e);
    }
  }

  /**
   * Vectorize the '$vectorize' fields in the update clause
   *
   * @param updateClause - Update clause to be vectorized
   */
  public Uni<Boolean> vectorizeUpdateClause(UpdateClause updateClause) {
    try {
      if (updateClause == null) return Uni.createFrom().item(true);
      final ObjectNode setNode = updateClause.updateOperationDefs().get(UpdateOperator.SET);
      final ObjectNode setOnInsertNode =
          updateClause.updateOperationDefs().get(UpdateOperator.SET_ON_INSERT);
      return updateVectorize(setNode)
          .onItem()
          .transformToUni(
              vectorized -> {
                return updateVectorize(setOnInsertNode);
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
    } catch (JsonApiException e) {
      return Uni.createFrom().failure(e);
    }
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
        if (embeddingProvider == null) {
          throw ErrorCode.EMBEDDING_SERVICE_NOT_CONFIGURED.toApiException(
              schemaObject.name.table());
        }
        if (text.isBlank()) {
          node.putNull(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
        } else {
          final Uni<List<float[]>> vectors =
              embeddingProvider
                  .vectorize(
                      1,
                      List.of(text),
                      embeddingApiKey,
                      EmbeddingProvider.EmbeddingRequestType.INDEX)
                  .map(res -> res.embeddings());
          return vectors
              .onItem()
              .transform(
                  vectorData -> {
                    float[] vector = vectorData.get(0);
                    // check if vector have the expected size
                    if (vector.length != schemaObject.vectorConfig().vectorSize()) {
                      throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                          "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                          schemaObject.vectorConfig().vectorizeConfig().provider(),
                          schemaObject.vectorConfig().vectorSize(),
                          vector.length);
                    }
                    final ArrayNode arrayNode = nodeFactory.arrayNode(vector.length);
                    for (float listValue : vector) {
                      arrayNode.add(nodeFactory.numberNode(listValue));
                    }
                    node.put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, arrayNode);
                    return true;
                  });
        }

      } else {
        throw new JsonApiException(
            ErrorCode.SHRED_BAD_VECTORIZE_VALUE, ErrorCode.SHRED_BAD_VECTORIZE_VALUE.getMessage());
      }
    }
    return Uni.createFrom().item(true);
  }
}
