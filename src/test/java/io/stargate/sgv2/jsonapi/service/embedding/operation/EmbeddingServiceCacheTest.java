package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PropertyBasedOverrideProfile.class)
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
}
