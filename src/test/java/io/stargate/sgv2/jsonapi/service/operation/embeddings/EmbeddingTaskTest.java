package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.billing.Billing;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.tasks.Task;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ModelType;
import io.stargate.sgv2.jsonapi.service.provider.ModelUsage;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link EmbeddingTask} */
public class EmbeddingTaskTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();

  private static final int DIMENSION = 3;
  private static final VectorizeDefinition VECTORIZE_DEF =
      new VectorizeDefinition("nvidia", "nvidia/nv-embedqa-e5-v5", null, null);
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private CommandContext<CollectionSchemaObject> commandContext;
  private Billing billing;
  private EmbeddingProvider embeddingProvider;

  @BeforeEach
  public void setUp() {
    billing = mock(Billing.class);
    embeddingProvider = mock(EmbeddingProvider.class);

    // context is all mocks, re-stub to get our billing and provider
    commandContext = TEST_CONSTANTS.collectionContext();
    when(commandContext.requestContext().billing()).thenReturn(billing);
    when(commandContext
            .embeddingProviderFactory()
            .create(any(), any(), any(), any(), anyInt(), any(), any(), any()))
        .thenReturn(embeddingProvider);
  }

  @Test
  public void billsProviderUsage() {
    var modelUsage = modelUsage();
    var vectors = List.of(new float[] {0.1f, 0.2f, 0.3f}, new float[] {0.4f, 0.5f, 0.6f});
    providerReturns(vectors, modelUsage);

    var received = new ArrayList<float[]>();
    var task = buildTask(List.of(action("first", received), action("second", received)));
    Task<CollectionSchemaObject> executed = task.execute(commandContext).await().atMost(TIMEOUT);

    assertThat(executed.status()).isEqualTo(Task.TaskStatus.COMPLETED);
    assertThat(received).containsExactlyElementsOf(vectors);
    verify(billing).emitEvent(same(modelUsage));
    verifyNoMoreInteractions(billing);
  }

  @Test
  public void noBillingWhenProviderFails() {
    when(embeddingProvider.vectorize(anyInt(), anyList(), any(), any()))
        .thenReturn(Uni.createFrom().failure(new RuntimeException("provider unavailable")));

    var task = buildTask(List.of(action("first", new ArrayList<>())));
    Task<CollectionSchemaObject> executed = task.execute(commandContext).await().atMost(TIMEOUT);

    assertThat(executed.status()).isEqualTo(Task.TaskStatus.ERROR);
    verifyNoInteractions(billing);
  }

  @Test
  public void billsWhenResponseRejected() {
    // one vector for two texts fails the task, but the provider still used the tokens
    var modelUsage = modelUsage();
    providerReturns(List.of(new float[] {0.1f, 0.2f, 0.3f}), modelUsage);

    var received = new ArrayList<float[]>();
    var task = buildTask(List.of(action("first", received), action("second", received)));
    Task<CollectionSchemaObject> executed = task.execute(commandContext).await().atMost(TIMEOUT);

    assertThat(executed.status()).isEqualTo(Task.TaskStatus.ERROR);
    assertThat(received).isEmpty();
    verify(billing).emitEvent(same(modelUsage));
    verifyNoMoreInteractions(billing);
  }

  private void providerReturns(List<float[]> vectors, ModelUsage modelUsage) {
    when(embeddingProvider.vectorize(anyInt(), anyList(), any(), any()))
        .thenReturn(
            Uni.createFrom()
                .item(new EmbeddingProvider.BatchedEmbeddingResponse(1, vectors, modelUsage)));
  }

  private EmbeddingTask<CollectionSchemaObject> buildTask(List<EmbeddingDeferredAction> actions) {
    return EmbeddingTask.builder(commandContext)
        .withDimension(DIMENSION)
        .withVectorizeDefinition(VECTORIZE_DEF)
        .withEmbeddingActions(actions)
        .withRetryPolicy(TaskRetryPolicy.NO_RETRY)
        .withOriginalCommandName("insertMany")
        .withRequestType(EmbeddingProvider.EmbeddingRequestType.INDEX)
        .build();
  }

  private static EmbeddingDeferredAction action(String text, List<float[]> received) {
    return new EmbeddingDeferredAction(text, DIMENSION, VECTORIZE_DEF, received::add, e -> {});
  }

  private static ModelUsage modelUsage() {
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        VECTORIZE_DEF.modelName(),
        TEST_CONSTANTS.TENANT,
        ModelInputType.INDEX,
        12,
        12,
        256,
        1024,
        1_000L);
  }
}
