package io.stargate.sgv2.jsonapi.service.embedding;

import static io.stargate.sgv2.jsonapi.exception.ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
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
  private final EmbeddingCredentials embeddingCredentials;
  private final SchemaObject schemaObject;

  /**
   * Constructor
   *
   * @param embeddingProvider - Service client based on embedding service configuration set for the
   *     table
   * @param nodeFactory - Jackson node factory to create json nodes added to the document
   * @param embeddingCredentials - Credentials for the embedding service
   * @param schemaObject - The collection setting for vectorize call
   */
  public DataVectorizer(
      EmbeddingProvider embeddingProvider,
      JsonNodeFactory nodeFactory,
      EmbeddingCredentials embeddingCredentials,
      SchemaObject schemaObject) {
    this.embeddingProvider = embeddingProvider;
    this.nodeFactory = nodeFactory;
    this.embeddingCredentials = embeddingCredentials;
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
            throw ErrorCodeV1.INVALID_USAGE_OF_VECTORIZE.toApiException(
                "issue in document at position %d", (position + 1));
          }
          final JsonNode jsonNode =
              document.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
          if (jsonNode.isNull()) {
            ((ObjectNode) document)
                .put(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD, (String) null);
            continue;
          }
          if (!jsonNode.isTextual()) {
            throw ErrorCodeV1.INVALID_VECTORIZE_VALUE_TYPE.toApiException(
                "issue in document at position %s", (position + 1));
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
          throw ErrorCodeV1.EMBEDDING_SERVICE_NOT_CONFIGURED.toApiException(
              schemaObject.name().table());
        }
        Uni<List<float[]>> vectors =
            embeddingProvider
                .vectorize(
                    1,
                    vectorizeTexts,
                    embeddingCredentials,
                    EmbeddingProvider.EmbeddingRequestType.INDEX)
                .map(res -> res.embeddings());
        return vectors
            .onItem()
            .transform(
                vectorData -> {
                  final VectorConfig vectorConfig = schemaObject.vectorConfig();
                  // This will be the first element for collection
                  final VectorConfig.ColumnVectorDefinition collectionVectorDefinition =
                      vectorConfig.columnVectorDefinitions().get(0);

                  // check if we get back the same number of vectors that we asked for
                  if (vectorData.size() != vectorizeTexts.size()) {
                    throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                        "Embedding provider '%s' didn't return the expected number of embeddings. Expect: '%d'. Actual: '%d'",
                        collectionVectorDefinition.vectorizeDefinition().provider(),
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
                    if (vector.length != collectionVectorDefinition.vectorSize()) {
                      throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                          "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                          collectionVectorDefinition.vectorizeDefinition().provider(),
                          collectionVectorDefinition.vectorSize(),
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
   * This method will be used to vectorize the $vectorize string content vectorizeContent must be
   * not null and not blank text
   *
   * @param vectorizeContent - vectorize string to be vectorized
   * @return Uni<float[]> - result vector float array
   */
  public Uni<float[]> vectorize(String vectorizeContent) {
    Uni<List<float[]>> vectors =
        embeddingProvider
            .vectorize(
                1,
                List.of(vectorizeContent),
                embeddingCredentials,
                EmbeddingProvider.EmbeddingRequestType.INDEX)
            .map(EmbeddingProvider.Response::embeddings);
    return vectors
        .onItem()
        .transform(
            vectorData -> {
              final VectorConfig vectorConfig = schemaObject.vectorConfig();
              // This will be the first element for collection
              final VectorConfig.ColumnVectorDefinition collectionVectorDefinition =
                  vectorConfig.columnVectorDefinitions().get(0);
              float[] vector = vectorData.get(0);
              // check if vector have the expected size
              if (vector.length != collectionVectorDefinition.vectorSize()) {
                throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                    "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                    collectionVectorDefinition.vectorizeDefinition().provider(),
                    collectionVectorDefinition.vectorSize(),
                    vector.length);
              }
              return vector;
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
          throw ErrorCodeV1.EMBEDDING_SERVICE_NOT_CONFIGURED.toApiException(
              schemaObject.name().table());
        }
        Uni<List<float[]>> vectors =
            embeddingProvider
                .vectorize(
                    1,
                    List.of(text),
                    embeddingCredentials,
                    EmbeddingProvider.EmbeddingRequestType.SEARCH)
                .map(res -> res.embeddings());
        return vectors
            .onItem()
            .transform(
                vectorData -> {
                  float[] vector = vectorData.get(0);
                  final VectorConfig vectorConfig = schemaObject.vectorConfig();
                  // This will be the first element for collection
                  final VectorConfig.ColumnVectorDefinition collectionVectorDefinition =
                      vectorConfig.columnVectorDefinitions().get(0);
                  // check if vector have the expected size
                  if (vector.length != collectionVectorDefinition.vectorSize()) {
                    throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                        "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
                        collectionVectorDefinition.vectorizeDefinition().provider(),
                        collectionVectorDefinition.vectorSize(),
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
