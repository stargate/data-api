package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen model.
 */
public abstract class EmbeddingProvider {
  protected static final Logger logger = LoggerFactory.getLogger(EmbeddingProvider.class);
  protected EmbeddingProviderConfigStore.RequestProperties requestProperties;
  protected String baseUrl;
  protected String modelName;
  protected int dimension;
  protected Map<String, Object> vectorizeServiceParameters;

  // default constructor
  protected EmbeddingProvider() {}

  protected EmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    this.requestProperties = requestProperties;
    this.baseUrl = baseUrl;
    this.modelName = modelName;
    this.dimension = dimension;
    this.vectorizeServiceParameters = vectorizeServiceParameters;
  }

  /**
   * Vectorizes the given list of texts and returns the embeddings.
   *
   * @param texts List of texts to be vectorized
   * @param apiKeyOverride Optional API key to be used for this request. If not provided, the
   *     default API key will be used.
   * @param embeddingRequestType Type of request (INDEX or SEARCH)
   * @return VectorResponse
   */
  public abstract Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType);

  /**
   * returns the maximum batch size supported by the provider
   *
   * @return
   */
  public abstract int maxBatchSize();

  /**
   * Record to hold the batchId and embedding vectors
   *
   * @param batchId - Sequence number for the batch to order the vectors.
   * @param embeddings - Embedding vectors for the given text inputs.
   */
  public record Response(int batchId, List<float[]> embeddings) {
    public static Response of(int batchId, List<float[]> embeddings) {
      return new Response(batchId, embeddings);
    }
  }

  public enum EmbeddingRequestType {
    /** This is used when vectorizing data in write operation for indexing */
    INDEX,
    /** This is used when vectorizing data for search operation */
    SEARCH
  }
}
