package io.stargate.sgv2.jsonapi.service.embedding;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;
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
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiColumnDef;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTypeName;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorType;
import java.util.*;

/**
 * Utility class to execute embedding service to get vector embeddings for the text fields in the
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
          if (document.has(VECTOR_EMBEDDING_FIELD)) {
            throw ErrorCodeV1.INVALID_USAGE_OF_VECTORIZE.toApiException(
                "issue in document at position %d", (position + 1));
          }
          final JsonNode jsonNode =
              document.get(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
          if (jsonNode.isNull()) {
            ((ObjectNode) document).put(VECTOR_EMBEDDING_FIELD, (String) null);
            continue;
          }
          if (!jsonNode.isTextual()) {
            throw ErrorCodeV1.INVALID_VECTORIZE_VALUE_TYPE.toApiException(
                "issue in document at position %s", (position + 1));
          }

          String vectorizeData = jsonNode.asText();
          if (vectorizeData.isBlank()) {
            ((ObjectNode) document).put(VECTOR_EMBEDDING_FIELD, (String) null);
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
                  // TODO: AARON - this code had no null projection, now throws if not present
                  final VectorColumnDefinition collectionVectorDefinition =
                      vectorConfig.getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD).orElseThrow();

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
                    ((ObjectNode) document).set(VECTOR_EMBEDDING_FIELD, arrayNode);
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
    if (embeddingProvider == null) {
      throw ErrorCodeV1.EMBEDDING_SERVICE_NOT_CONFIGURED.toApiException(
          schemaObject.name().table());
    }
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
              // TODO: AARON - this code had no null projection, now throws if not present
              final VectorColumnDefinition collectionVectorDefinition =
                  vectorConfig.getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD).orElseThrow();
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
                  // TODO: AARON - this code had no null projection, now throws if not present
                  final VectorColumnDefinition collectionVectorDefinition =
                      vectorConfig.getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD).orElseThrow();
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

  public <T> Uni<T> vectorizeTasks(
      T uniObject, List<VectorizeTask> tasks, EmbeddingProvider.EmbeddingRequestType requestType) {
    Objects.requireNonNull(tasks, "tasks must not be null");

    if (tasks.isEmpty()) {
      return Uni.createFrom().item(uniObject);
    }

    // HACK: currently we only support one embedding provider because collections only supported one
    // so quick check here that we only have one, and then use it's name as the name to blame if
    // anything
    // goes wrong :)
    var embeddingProviderNames =
        tasks.stream()
            .map(task -> task.vectorType.getVectorizeDefinition().provider())
            .distinct()
            .toList();
    if (embeddingProviderNames.size() != 1) {
      throw new IllegalArgumentException(
          "Must be single embedding provider name, got " + embeddingProviderNames);
    }

    var textToVectorize = tasks.stream().map(VectorizeTask::getVectorizeText).toList();

    return vectorizeTexts(textToVectorize, embeddingProviderNames.getFirst(), requestType)
        .onItem()
        .transform(
            vectorData -> {
              for (int i = 0; i < vectorData.size(); i++) {
                tasks.get(i).setVector(vectorData.get(i));
              }
              return uniObject;
            });
  }

  private Uni<List<float[]>> vectorizeTexts(
      List<String> textsToVectorize,
      String providerName,
      EmbeddingProvider.EmbeddingRequestType requestType) {

    // Copied from vectorize(List<JsonNode> documents) above leaving as is for now
    if (embeddingProvider == null) {
      throw ErrorCodeV1.EMBEDDING_SERVICE_NOT_CONFIGURED.toApiException(
          schemaObject.name().table());
    }

    return embeddingProvider
        .vectorize(1, textsToVectorize, embeddingCredentials, requestType)
        .map(EmbeddingProvider.Response::embeddings)
        .onItem()
        .transform(
            vectorData -> {
              // Copied from vectorize(List<JsonNode> documents) above leaving as is for now
              if (vectorData.size() != textsToVectorize.size()) {
                throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                    "Embedding provider '%s' didn't return the expected number of embeddings. Expect: '%d'. Actual: '%d'",
                    providerName, textsToVectorize.size(), vectorData.size());
              }
              return vectorData;
            });
  }

  /**
   * A task to vectorize a single field in a document
   *
   * <p>NOTE: aaron - 5 - nove 2024 this was added for tables so we did not change the collection
   * code, we should refactor to use this pattern for the collection code as well.
   */
  public static class VectorizeTask {

    private final ObjectNode parentObject;
    final ApiColumnDef columnDef;
    final ApiVectorType vectorType;

    public VectorizeTask(ObjectNode parentObject, ApiColumnDef columnDef) {
      this.parentObject = parentObject;
      // Parent can be null if a subclass wants to handle updating the target.
      this.columnDef = columnDef;
      // sanity checks

      if (columnDef.type().typeName() != ApiTypeName.VECTOR) {
        throw new IllegalArgumentException(
            "Column must be of type VECTOR, columnDef: " + columnDef);
      }
      this.vectorType = (ApiVectorType) columnDef.type();
      Objects.requireNonNull(
          vectorType.getVectorizeDefinition(),
          "vectorType.getVectorizeDefinition() must not be null");
    }

    /**
     * Call to get the text this task wants to be vectorized
     *
     * @return Text to be vectorized
     */
    public String getVectorizeText() {
      return parentObject.get(columnDef.jsonKey()).asText();
    }

    /**
     * Call to get this task to set the vector that was created to replace the text.
     *
     * @param vector vector to replace the text with
     */
    public void setVector(float[] vector) {
      validateVector(vector);
      updateTarget(vector);
    }

    /** Validate the vector length against the definition of what we expected. */
    private void validateVector(float[] vector) {
      // Copied from vectorize(List<JsonNode> documents) above leaving as is for now - aaron
      if (vector.length != vectorType.getDimension()) {
        throw EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
            "Embedding provider '%s' did not return expected embedding length. Expect: '%d'. Actual: '%d'",
            vectorType.getVectorizeDefinition().provider(),
            vectorType.getDimension(),
            vector.length);
      }
    }

    /**
     * Update the target with the vector, subclasses should override this if they want to write the
     * vector somewhere
     */
    private void updateTarget(float[] vector) {
      Objects.requireNonNull(parentObject, "parentObject must not be null");

      var arrayNode = JsonNodeFactory.instance.arrayNode(vector.length);
      for (float v : vector) {
        arrayNode.add(JsonNodeFactory.instance.numberNode(v));
      }

      parentObject.set(columnDef.jsonKey(), arrayNode);
    }
  }

  /** Vector class for a sort expression */
  public static class SortVectorizeTask extends VectorizeTask {

    private final SortClause sortClause;
    private final SortExpression sortExpression;

    SortVectorizeTask(
        SortClause sortClause, SortExpression sortExpression, ApiColumnDef columnDef) {
      super(null, columnDef);
      this.sortClause = Objects.requireNonNull(sortClause, "sortClause must not be null");
      this.sortExpression =
          Objects.requireNonNull(sortExpression, "sortExpression must not be null");
    }

    @Override
    public String getVectorizeText() {
      return sortExpression.vectorize();
    }

    @Override
    public void setVector(float[] vector) {
      // Changes from vectorize(SortClause sortClause) above, it cleared the list this is replacing
      // the
      // sort expression in the list.
      var i = sortClause.sortExpressions().indexOf(sortExpression);
      sortClause
          .sortExpressions()
          .set(i, SortExpression.tableVectorSort(sortExpression.path(), vector));
    }
  }
}
