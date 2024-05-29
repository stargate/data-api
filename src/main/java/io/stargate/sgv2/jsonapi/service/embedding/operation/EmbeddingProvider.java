package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Optional;

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen model.
 */
public interface EmbeddingProvider {
  /**
   * Vectorizes the given list of texts and returns the embeddings.
   *
   * @param texts List of texts to be vectorized
   * @param apiKeyOverride Optional API key to be used for this request. If not provided, the
   *     default API key will be used.
   * @param embeddingRequestType Type of request (INDEX or SEARCH)
   * @return VectorResponse
   */
  Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType);

  /**
   * returns the supported batch size from the provider
   *
   * @return
   */
  int batchSize();

  /**
   * Record to hold the batchId and embedding vectors
   *
   * @param batchId - Sequence number for the batch to order the vectors.
   * @param embeddings - Embedding vectors for the given text inputs.
   */
  record Response(int batchId, List<float[]> embeddings) {
    public static Response of(int batchId, List<float[]> embeddings) {
      return new Response(batchId, embeddings);
    }
  }

  enum EmbeddingRequestType {
    /** This is used when vectorizing data in write operation for indexing */
    INDEX,
    /** This is used when vectorizing data for search operation */
    SEARCH
  }
}
