package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(EmbeddingServiceCacheTest.PropertyBasedOverrideProfile.class)
public class EmbeddingServiceCacheTest {

  @Inject EmbeddingServiceCache embeddingServiceCache;

  @Nested
  class ServiceCache {

    @Test
    public void enabledService() {
      // check that the embedding service is returned based on property
      final EmbeddingService openai =
          embeddingServiceCache.getConfiguration(Optional.empty(), "openai", "openai-model");
      assertThat(openai).isNotNull();
      assertThat(openai).isInstanceOf(OpenAiEmbeddingClient.class);

      // check that the embedding service cache returns the same OpenAiEmbeddingClient instance for
      // same tenant and service name
      final EmbeddingService openaiReuse =
          embeddingServiceCache.getConfiguration(Optional.empty(), "openai", "openai-model");
      assertThat(openaiReuse).isNotNull();
      assertThat(openaiReuse).isInstanceOf(OpenAiEmbeddingClient.class);
      assertThat(openaiReuse).isEqualTo(openai);

      // check that the embedding service cache returns the different OpenAiEmbeddingClient instance
      // for different tenant or service name
      final EmbeddingService different =
          embeddingServiceCache.getConfiguration(
              Optional.of(UUID.randomUUID().toString()), "openai", "openai-model");
      assertThat(different).isNotNull();
      assertThat(different).isInstanceOf(OpenAiEmbeddingClient.class);
      assertThat(different).isNotEqualTo(openai);

      // Try a different service provider
      final EmbeddingService huggingface =
          embeddingServiceCache.getConfiguration(
              Optional.empty(), "huggingface", "huggingface-model");
      assertThat(huggingface).isNotNull();
      assertThat(huggingface).isInstanceOf(HuggingFaceEmbeddingClient.class);
    }

    @Test
    public void unConfiguredEnabledService() {
      Throwable failure =
          catchThrowable(
              () ->
                  embeddingServiceCache.getConfiguration(
                      Optional.empty(), "vertexai", "vertexai-model"));
      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED)
          .hasFieldOrPropertyWithValue("message", "Vectorize service type not enabled : vertexai");
    }
  }

  public static class PropertyBasedOverrideProfile implements QuarkusTestProfile {
    @Override
    public boolean disableGlobalTestResources() {
      return true;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.jsonapi.embedding.config.store", "property")
          .put("stargate.jsonapi.embedding.service.openai.enabled", "true")
          .put("stargate.jsonapi.embedding.service.openai.api-key", "openai-api-key")
          .put("stargate.jsonapi.embedding.service.openai.model-name", "openai-model-name")
          .put("stargate.jsonapi.embedding.service.openai.url", "https://api.openai.com/v1/")
          .put("stargate.jsonapi.embedding.service.hf.enabled", "true")
          .put("stargate.jsonapi.embedding.service.hf.api-key", "hf-api-key")
          .put("stargate.jsonapi.embedding.service.hf.model-name", "hf-model-name")
          .put("stargate.jsonapi.embedding.service.hf.url", "https://api-inference.huggingface.co")
          .build();
    }
  }
}
