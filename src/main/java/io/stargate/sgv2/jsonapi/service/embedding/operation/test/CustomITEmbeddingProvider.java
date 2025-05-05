package io.stargate.sgv2.jsonapi.service.embedding.operation.test;

import io.quarkus.runtime.annotations.RegisterForReflection;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import java.util.*;

/**
 * This is a test implementation of the EmbeddingProvider interface. It is used for
 * VectorizeSearchIntegrationTest
 */
@RegisterForReflection
public class CustomITEmbeddingProvider extends EmbeddingProvider {

  public static final String TEST_API_KEY = "test_embedding_service_api_key";

  public static final List<String> SAMPLE_VECTORIZE_CONTENTS =
      List.of(
          "ChatGPT integrated sneakers that talk to you",
          "ChatGPT upgraded",
          "New data updated",
          "An AI quilt to help you sleep forever",
          "A deep learning display that controls your mood",
          "Updating new data");

  public static HashMap<String, float[]> TEST_DATA_DIMENSION_5 = new HashMap<>();
  public static HashMap<String, float[]> TEST_DATA_DIMENSION_6 = new HashMap<>();

  private final int dimension;

  public CustomITEmbeddingProvider(int dimension) {
    // construct the test modelConfig
    super(
        null,
        null,
        new EmbeddingProvidersConfigImpl.EmbeddingProviderConfigImpl.ModelConfigImpl(
            "testModel",
            new ApiModelSupport.ApiModelSupportImpl(
                ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
            Optional.of(dimension),
            List.of(),
            Map.of(),
            Optional.empty()),
        dimension,
        Map.of());
    this.dimension = dimension;
  }

  static {
    TEST_DATA_DIMENSION_5.put(
        "ChatGPT integrated sneakers that talk to you",
        new float[] {0.1f, 0.15f, 0.3f, 0.12f, 0.05f});
    TEST_DATA_DIMENSION_5.put("ChatGPT upgraded", new float[] {0.1f, 0.16f, 0.31f, 0.22f, 0.15f});
    TEST_DATA_DIMENSION_5.put("New data updated", new float[] {0.15f, 0.16f, 0.35f, 0.22f, 0.55f});
    TEST_DATA_DIMENSION_5.put(
        "An AI quilt to help you sleep forever", new float[] {0.45f, 0.09f, 0.01f, 0.2f, 0.11f});
    TEST_DATA_DIMENSION_5.put(
        "A deep learning display that controls your mood",
        new float[] {0.1f, 0.05f, 0.08f, 0.3f, 0.6f});
    TEST_DATA_DIMENSION_5.put("Updating new data", new float[] {0.22f, 0.55f, 0.68f, 0.36f, 0.6f});
  }

  static {
    TEST_DATA_DIMENSION_6.put(
        "ChatGPT integrated sneakers that talk to you",
        new float[] {0.1f, 0.15f, 0.3f, 0.12f, 0.05f, 0.05f});
    TEST_DATA_DIMENSION_6.put(
        "ChatGPT upgraded", new float[] {0.1f, 0.16f, 0.31f, 0.22f, 0.15f, 0.05f});
    TEST_DATA_DIMENSION_6.put(
        "New data updated", new float[] {0.15f, 0.16f, 0.35f, 0.22f, 0.55f, 0.05f});
    TEST_DATA_DIMENSION_6.put(
        "An AI quilt to help you sleep forever",
        new float[] {0.45f, 0.09f, 0.01f, 0.2f, 0.11f, 0.05f});
    TEST_DATA_DIMENSION_6.put(
        "A deep learning display that controls your mood",
        new float[] {0.1f, 0.05f, 0.08f, 0.3f, 0.6f, 0.05f});
    TEST_DATA_DIMENSION_6.put(
        "Updating new data", new float[] {0.22f, 0.55f, 0.68f, 0.36f, 0.6f, 0.05f});
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    // Check if using an EOF model
    checkEOLModelUsage();

    List<float[]> response = new ArrayList<>(texts.size());
    if (texts.size() == 0) return Uni.createFrom().item(Response.of(batchId, response));
    if (!embeddingCredentials.apiKey().isPresent()
        || !embeddingCredentials.apiKey().get().equals(TEST_API_KEY))
      return Uni.createFrom().failure(new RuntimeException("Invalid API Key"));
    for (String text : texts) {
      if (dimension == 5) {
        if (TEST_DATA_DIMENSION_5.containsKey(text)) {
          response.add(TEST_DATA_DIMENSION_5.get(text));
        } else {
          response.add(new float[] {0.25f, 0.25f, 0.25f, 0.25f, 0.25f});
        }
      }
      if (dimension == 6) {
        if (TEST_DATA_DIMENSION_6.containsKey(text)) {
          response.add(TEST_DATA_DIMENSION_6.get(text));
        } else {
          response.add(new float[] {0.25f, 0.25f, 0.25f, 0.25f, 0.25f, 0.05f});
        }
      }
    }
    return Uni.createFrom().item(Response.of(batchId, response));
  }

  @Override
  public int maxBatchSize() {
    return 1;
  }
}
