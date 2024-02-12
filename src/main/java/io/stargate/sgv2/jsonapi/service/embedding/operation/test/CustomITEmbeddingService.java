package io.stargate.sgv2.jsonapi.service.embedding.operation.test;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * This is a test implementation of the EmbeddingService interface. It is used for
 * VectorizeSearchIntegrationTest
 */
@RegisterForReflection
public class CustomITEmbeddingService implements EmbeddingService {

  public static HashMap<String, float[]> TEST_DATA = new HashMap<>();

  static {
    TEST_DATA.put(
        "ChatGPT integrated sneakers that talk to you",
        new float[] {0.1f, 0.15f, 0.3f, 0.12f, 0.05f});
    TEST_DATA.put("ChatGPT upgraded", new float[] {0.1f, 0.16f, 0.31f, 0.22f, 0.15f});
    TEST_DATA.put("New data updated", new float[] {0.15f, 0.16f, 0.35f, 0.22f, 0.55f});
    TEST_DATA.put(
        "An AI quilt to help you sleep forever", new float[] {0.45f, 0.09f, 0.01f, 0.2f, 0.11f});
    TEST_DATA.put(
        "A deep learning display that controls your mood",
        new float[] {0.1f, 0.05f, 0.08f, 0.3f, 0.6f});
    TEST_DATA.put("Updating new data", new float[] {0.22f, 0.55f, 0.68f, 0.36f, 0.6f});
  }
  ;

  @Override
  public Uni<List<float[]>> vectorize(List<String> texts, Optional<String> apiKey) {
    List<float[]> response = new ArrayList<>(texts.size());
    for (String text : texts) {
      if (TEST_DATA.containsKey(text)) {
        response.add(TEST_DATA.get(text));
      } else {
        response.add(new float[] {0.25f, 0.25f, 0.25f, 0.25f, 0.25f});
      }
    }
    return Uni.createFrom().item(response);
  }
}
