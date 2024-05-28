package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;

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
   * @return List of embeddings for the given texts
   */
  Uni<Pair<Integer, List<float[]>>> vectorize(
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

  enum EmbeddingRequestType {
    /** This is used when vectorizing data in write operation for indexing */
    INDEX,
    /** This is used when vectorizing data for search operation */
    SEARCH
  }
}
