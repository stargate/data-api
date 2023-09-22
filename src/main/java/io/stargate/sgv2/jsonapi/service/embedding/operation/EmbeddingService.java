package io.stargate.sgv2.jsonapi.service.embedding.operation;

import java.util.List;

public interface EmbeddingService {
  List<float[]> vectorize(List<String> texts);
}
