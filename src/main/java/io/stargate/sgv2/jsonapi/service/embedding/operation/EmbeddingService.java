package io.stargate.sgv2.jsonapi.service.embedding.operation;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public interface EmbeddingService {
  int EXECUTOR_SERICE_THREADS = 10;
  int MAX_RETRIES = 3;
  ExecutorService executor =
      Executors.newFixedThreadPool(
          EXECUTOR_SERICE_THREADS, r -> new Thread(r, "EMBEDDING_SERVICE_REQUEST"));

  List<float[]> vectorize(List<String> texts);
}
