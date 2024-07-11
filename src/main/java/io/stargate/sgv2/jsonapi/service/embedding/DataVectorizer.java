package io.stargate.sgv2.jsonapi.service.embedding;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
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
  private final CollectionSettings collectionSettings;

  /**
   * Constructor
   *
   * @param embeddingProvider - Service client based on embedding service configuration set for the
   *     table
   * @param nodeFactory - Jackson node factory to create json nodes added to the document
   * @param embeddingApiKey - Optional override embedding api key came in request header
   * @param collectionSettings - The collection setting for vectorize call
   */
  public DataVectorizer(
      EmbeddingProvider embeddingProvider,
      JsonNodeFactory nodeFactory,
      String embeddingApiKey,
      CollectionSettings collectionSettings) {
    this.embeddingProvider = embeddingProvider;
    this.nodeFactory = nodeFactory;
    this.embeddingApiKey = embeddingApiKey;
    this.collectionSettings = collectionSettings;
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
              collectionSettings.collectionName());
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
                        collectionSettings.vectorConfig().vectorizeConfig().provider(),
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
                    if (vector.length != collectionSettings.vectorConfig().vectorSize()) {
                      throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                          "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                          collectionSettings.vectorConfig().vectorizeConfig().provider(),
                          collectionSettings.vectorConfig().vectorSize(),
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
   * This method will be used by documentUpdater(updateOne, updateMany, findOneAndUpdate,
   * findOneAndReplace) Since we need to vectorize on demand, so vectorization for updateCommands
   * will postpone and move into ReadAndUpdateOperation.
   *
   * @param document - Document to be vectorized
   * @return Uni<Boolean> - have modified the document or not
   */
  public Uni<Boolean> vectorizeUpdateDocument(JsonNode document) {
    if (!document.has(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
      return Uni.createFrom().item(false);
    }
    final JsonNode jsonNode = document.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
    // $vectorize as null value, also update $vector as null, modified
    if (jsonNode.isNull()) {
      ((ObjectNode) document).put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
      return Uni.createFrom().item(true);
    }
    // $vectorize is not textual value
    if (!jsonNode.isTextual()) {
      throw ErrorCode.INVALID_VECTORIZE_VALUE_TYPE.toApiException();
    }
    String vectorizeData = jsonNode.asText();
    // $vectorize is blank text value, set $vector as null value, modified
    if (vectorizeData.isBlank()) {
      ((ObjectNode) document).put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
      return Uni.createFrom().item(true);
    }

    // $vectorize is textual and not blank, going to vectorize it
    if (embeddingProvider == null) {
      throw ErrorCode.EMBEDDING_SERVICE_NOT_CONFIGURED.toApiException(
          collectionSettings.collectionName());
    }
    Uni<List<float[]>> vectors =
        embeddingProvider
            .vectorize(
                1,
                List.of(vectorizeData),
                embeddingApiKey,
                EmbeddingProvider.EmbeddingRequestType.INDEX)
            .map(EmbeddingProvider.Response::embeddings);
    return vectors
        .onItem()
        .transform(
            vectorData -> {
              float[] vector = vectorData.get(0);
              // check if vector have the expected size
              if (vector.length != collectionSettings.vectorConfig().vectorSize()) {
                throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                    "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                    collectionSettings.vectorConfig().vectorizeConfig().provider(),
                    collectionSettings.vectorConfig().vectorSize(),
                    vector.length);
              }
              final ArrayNode arrayNode = nodeFactory.arrayNode(vector.length);
              for (float listValue : vector) {
                arrayNode.add(nodeFactory.numberNode(listValue));
              }
              ((ObjectNode) document)
                  .put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, arrayNode);
              return true;
            });
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
              collectionSettings.collectionName());
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
                  if (vector.length != collectionSettings.vectorConfig().vectorSize()) {
                    throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                        "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                        collectionSettings.vectorConfig().vectorizeConfig().provider(),
                        collectionSettings.vectorConfig().vectorSize(),
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
}
