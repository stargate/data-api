package io.stargate.sgv2.jsonapi.service.reranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.service.provider.ModelUsage;
import io.stargate.sgv2.jsonapi.service.provider.ProviderType;
import io.stargate.sgv2.jsonapi.service.reranking.operation.NvidiaRerankingProvider;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Tests for the RerankEGWClient class. Mocking the embedding gateway service to test the grpc
 * rerank API.
 */
@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class NvidiaRerankingClientTest {

  private static final RerankingCredentials RERANK_CREDENTIALS =
      new RerankingCredentials(Optional.of("mocked data api token"));

  @Test
  void handleValidResponse() {
    NvidiaRerankingProvider nvidiaRerankingProvider = mock(NvidiaRerankingProvider.class);
    when(nvidiaRerankingProvider.rerank(anyInt(), any(), any(), any()))
        .thenAnswer(
            invocation -> {
              List<RerankingProvider.Rank> ranks =
                  IntStream.range(0, 2)
                      .mapToObj(i -> new RerankingProvider.Rank(i, i == 0 ? 0.1f : 1f))
                      .toList();
              return Uni.createFrom()
                  .item(
                      new RerankingProvider.RerankingBatchResponse(
                          1,
                          ranks,
                          new ModelUsage(
                              ProviderType.RERANKING_PROVIDER,
                              "nvidia",
                              "llama-3.2-nv-rerankqa-1b-v2")));
            });

    final RerankingProvider.RerankingBatchResponse response =
        nvidiaRerankingProvider
            .rerank(1, "apple", List.of("orange", "apple"), RERANK_CREDENTIALS)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    assertThat(response).isNotNull();
    assertThat(response.batchId()).isEqualTo(1);
    assertThat(response.ranks()).isNotEmpty();
    assertThat(response.ranks().size()).isEqualTo(2);
    assertThat(response.ranks().get(0).index()).isEqualTo(0);
    assertThat(response.ranks().get(0).score()).isEqualTo(0.1f);
    assertThat(response.ranks().get(1).index()).isEqualTo(1);
    assertThat(response.ranks().get(1).score()).isEqualTo(1f);
  }
}
