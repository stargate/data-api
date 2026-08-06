package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class RerankingProviderTest {

  private static final TestConstants testConstants = new TestConstants();

  private static final RerankingCredentials RERANK_CREDENTIALS =
      new RerankingCredentials(testConstants.TENANT, "mocked reranking api key");

  @Test
  @SuppressWarnings("unchecked")
  void createPassageBatchesTest() {
    // mock a test reranking provider with maxBatchSize configured to 3
    TestRerankingProvider mockRerankingProvider = new TestRerankingProvider(3);
    // mock 11 passages
    List<String> passages =
        List.of(
            "orange",
            "apple",
            "banana",
            "grape",
            "kiwi",
            "mango",
            "pear",
            "peach",
            "plum",
            "pineapple",
            "strawberry");

    // invoke the private method createPassageBatches
    try {
      java.lang.reflect.Method method =
          RerankingProvider.class.getDeclaredMethod("createPassageBatches", List.class);
      method.setAccessible(true);
      List<List<String>> batches =
          (List<List<String>>) method.invoke(mockRerankingProvider, passages);
      assertThat(batches).hasSize(4);
      assertThat(batches.get(0)).hasSize(3);
      assertThat(batches.get(1)).hasSize(3);
      assertThat(batches.get(2)).hasSize(3);
      assertThat(batches.get(3)).hasSize(2);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void microBatchingTest() {
    // mock a test reranking provider with maxBatchSize configured to 10
    TestRerankingProvider mockRerankingProvider = new TestRerankingProvider(10);

    // mock query string
    String query = "apple";
    // mock 15 passages, which will be split into 2 micro batches
    List<String> passages =
        List.of(
            "orange",
            "apple",
            "banana",
            "grape",
            "kiwi",
            "mango",
            "pear",
            "peach",
            "plum",
            "pineapple",
            "strawberry",
            "blueberry",
            "raspberry",
            "watermelon",
            "cherry");

    final RerankingProvider.RerankingResponse finalResult =
        mockRerankingProvider
            .rerank(query, passages, RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // check if the final result contains all 15 passages
    assertThat(finalResult.ranks().size()).isEqualTo(passages.size());
    // assert the order of the passages in the final result should be the same as the original
    // passage order
    IntStream.range(0, 15)
        .forEach(i -> assertThat(finalResult.ranks().get(i).index()).isEqualTo(i));
  }

  @Test
  void primesFirstBatchBeforeStartingRemainingBatches() {
    ControlledRerankingProvider provider = new ControlledRerankingProvider();

    UniAssertSubscriber<RerankingProvider.RerankingResponse> subscriber =
        provider
            .rerank("query", List.of("first", "second", "third"), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());

    assertThat(provider.invokedBatchIds()).containsExactly(0);
    assertThat(provider.subscribedBatchIds()).containsExactly(0);

    provider.completeBatch(0);

    assertThat(provider.invokedBatchIds()).containsExactly(0, 1, 2);
    assertThat(provider.subscribedBatchIds()).containsExactly(0, 1, 2);
    assertThat(subscriber.getItem()).isNull();

    // Completing one tail batch must not complete the result: tail batches stay concurrent.
    provider.completeBatch(2);
    assertThat(subscriber.getItem()).isNull();
    provider.completeBatch(1);

    RerankingProvider.RerankingResponse result = subscriber.awaitItem().getItem();
    assertThat(result.ranks()).extracting(RerankingProvider.Rank::index).containsExactly(0, 1, 2);
    assertThat(result.modelUsage().batchCount()).isEqualTo(3);
  }

  @Test
  void doesNotStartRemainingBatchesWhenFirstBatchFails() {
    ControlledRerankingProvider provider = new ControlledRerankingProvider();
    RuntimeException expected = new RuntimeException("first batch failed");

    UniAssertSubscriber<RerankingProvider.RerankingResponse> subscriber =
        provider
            .rerank("query", List.of("first", "second", "third"), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());

    provider.failBatch(0, expected);

    assertThat(subscriber.awaitFailure().getFailure()).isSameAs(expected);
    assertThat(provider.invokedBatchIds()).containsExactly(0);
    assertThat(provider.subscribedBatchIds()).containsExactly(0);
  }

  @Test
  void handlesSingleBatchWithoutStartingTailBatches() {
    ControlledRerankingProvider provider = new ControlledRerankingProvider();

    UniAssertSubscriber<RerankingProvider.RerankingResponse> subscriber =
        provider
            .rerank("query", List.of("only"), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create());

    assertThat(provider.invokedBatchIds()).containsExactly(0);
    assertThat(provider.subscribedBatchIds()).containsExactly(0);

    provider.completeBatch(0);

    RerankingProvider.RerankingResponse result = subscriber.awaitItem().getItem();
    assertThat(result.ranks()).extracting(RerankingProvider.Rank::index).containsExactly(0);
    assertThat(result.modelUsage().batchCount()).isEqualTo(1);
  }

  @Test
  void preservesEmptyPassagesResult() {
    ControlledRerankingProvider provider = new ControlledRerankingProvider();

    RerankingProvider.RerankingResponse result =
        provider
            .rerank("query", List.of(), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    assertThat(provider.invokedBatchIds()).isEmpty();
    assertThat(provider.subscribedBatchIds()).isEmpty();
    assertThat(result.ranks()).isEmpty();
    assertThat(result.modelUsage()).isNull();
  }

  private static final class ControlledRerankingProvider extends TestRerankingProvider {

    private final Map<Integer, CompletableFuture<BatchedRerankingResponse>> batchResults =
        new ConcurrentHashMap<>();
    private final List<Integer> invokedBatchIds = new CopyOnWriteArrayList<>();
    private final List<Integer> subscribedBatchIds = new CopyOnWriteArrayList<>();

    private ControlledRerankingProvider() {
      super(1);
    }

    @Override
    public Uni<BatchedRerankingResponse> rerank(
        int batchId,
        String query,
        List<String> passages,
        RerankingCredentials rerankingCredentials) {
      invokedBatchIds.add(batchId);
      CompletableFuture<BatchedRerankingResponse> batchResult = new CompletableFuture<>();
      batchResults.put(batchId, batchResult);

      return Uni.createFrom()
          .deferred(
              () -> {
                subscribedBatchIds.add(batchId);
                return Uni.createFrom().completionStage(batchResult);
              });
    }

    private void completeBatch(int batchId) {
      batchResults
          .get(batchId)
          .complete(
              new BatchedRerankingResponse(
                  batchId,
                  List.of(new Rank(0, batchId)),
                  createEmptyModelUsage(RERANK_CREDENTIALS)));
    }

    private void failBatch(int batchId, Throwable failure) {
      batchResults.get(batchId).completeExceptionally(failure);
    }

    private List<Integer> invokedBatchIds() {
      return List.copyOf(invokedBatchIds);
    }

    private List<Integer> subscribedBatchIds() {
      return List.copyOf(subscribedBatchIds);
    }
  }
}
