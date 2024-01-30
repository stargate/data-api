package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import java.util.List;

public interface EmbeddingService {
  Uni<List<float[]>> vectorize(List<String> texts);
}
