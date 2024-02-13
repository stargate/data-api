package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import java.util.List;

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen model.
 */
public interface EmbeddingService {
  Uni<List<float[]>> vectorize(List<String> texts);
}
