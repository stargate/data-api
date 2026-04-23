package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.metrics.MetricsConstants;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MeteredEmbeddingProviderWrapperTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();

  private SimpleMeterRegistry meterRegistry;
  private JsonApiMetricsConfig metricsConfig;
  private RequestContext requestContext;

  @BeforeEach
  void setUp() {
    meterRegistry = new SimpleMeterRegistry();

    metricsConfig = mock(JsonApiMetricsConfig.class);
    when(metricsConfig.command()).thenReturn("command");
    when(metricsConfig.embeddingProvider()).thenReturn("embedding.provider");
    when(metricsConfig.embeddingModelTag()).thenReturn("embedding.model");
    when(metricsConfig.embeddingModelTagEnabled()).thenReturn(true);
    when(metricsConfig.vectorizeInputBytesMetrics()).thenReturn("vectorize.input.bytes");

    requestContext = TEST_CONSTANTS.requestContext();
  }

  @Test
  void shouldTagMetricsWithProviderApiName() {
    var provider = new TestEmbeddingProvider();
    var wrapper =
        new MeteredEmbeddingProviderWrapper(
            meterRegistry, metricsConfig, requestContext, provider, "testCommand");

    wrapper
        .vectorize(
            List.of("hello world"),
            TEST_CONSTANTS.EMBEDDING_CREDENTIALS,
            EmbeddingProvider.EmbeddingRequestType.INDEX)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem();

    // Verify that at least one metric was recorded with the provider API name tag
    List<Meter> meters = meterRegistry.getMeters();
    assertThat(meters).isNotEmpty();

    // Find the embedding.provider tag value across all registered meters
    var providerTagValues =
        meters.stream()
            .flatMap(m -> m.getId().getTags().stream())
            .filter(tag -> "embedding.provider".equals(tag.getKey()))
            .map(Tag::getValue)
            .distinct()
            .toList();

    // Should be the API name "custom", NOT the class name "TestEmbeddingProvider"
    assertThat(providerTagValues).containsExactly("custom");

    // Verify embedding.model tag is present with the model name
    var modelTagValues =
        meters.stream()
            .flatMap(m -> m.getId().getTags().stream())
            .filter(tag -> "embedding.model".equals(tag.getKey()))
            .map(Tag::getValue)
            .distinct()
            .toList();
    assertThat(modelTagValues).containsExactly("testModel");
  }

  @Test
  void shouldUseUnknownModelTagWhenDisabled() {
    when(metricsConfig.embeddingModelTagEnabled()).thenReturn(false);

    var provider = new TestEmbeddingProvider();
    var wrapper =
        new MeteredEmbeddingProviderWrapper(
            meterRegistry, metricsConfig, requestContext, provider, "testCommand");

    wrapper
        .vectorize(
            List.of("hello world"),
            TEST_CONSTANTS.EMBEDDING_CREDENTIALS,
            EmbeddingProvider.EmbeddingRequestType.INDEX)
        .subscribe()
        .withSubscriber(UniAssertSubscriber.create())
        .awaitItem();

    List<Meter> meters = meterRegistry.getMeters();
    var modelTagValues =
        meters.stream()
            .flatMap(m -> m.getId().getTags().stream())
            .filter(tag -> "embedding.model".equals(tag.getKey()))
            .map(Tag::getValue)
            .distinct()
            .toList();
    assertThat(modelTagValues).containsExactly(MetricsConstants.UNKNOWN_VALUE);
  }
}
