package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.Json;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfigImpl;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.testresource.RerankingTestResource;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Real provider HTTP serialization and batched response mapping without a database container. */
@QuarkusTest
@TestProfile(NvidiaRerankingHttpTest.Profile.class)
public class NvidiaRerankingHttpTest {

  public static class Profile implements NoGlobalResourcesTestProfile {
    @Override
    public List<TestResourceEntry> testResources() {
      // Disabling global resources also disables annotation discovery. Register only the HTTP
      // fixture explicitly so this test never starts a database container.
      return List.of(new TestResourceEntry(RerankingTestResource.class));
    }
  }

  private static final String MODEL = "nvidia/llama-3.2-nv-rerankqa-1b-v2";
  private static final TestConstants TEST_CONSTANTS = new TestConstants();
  private static final RerankingCredentials CREDENTIALS =
      new RerankingCredentials(TEST_CONSTANTS.TENANT, RerankingTestResource.API_KEY);

  private WireMockServer reranker;

  @BeforeEach
  void resetRequests() {
    reranker.resetRequests();
  }

  @Test
  void httpBatchesPreserveGlobalPassageIndicesAndAggregateUsage() {
    var passages =
        List.of(
            "score:1 first",
            "score:7 second",
            "score:3 third",
            "score:9 fourth",
            "score:2 fifth",
            "score:8 sixth",
            "score:4 seventh");
    var response =
        provider(3)
            .rerank("search query", passages, CREDENTIALS)
            .await()
            .atMost(Duration.ofSeconds(10));

    assertThat(response.ranks())
        .containsExactly(
            new RerankingProvider.Rank(0, 1),
            new RerankingProvider.Rank(1, 7),
            new RerankingProvider.Rank(2, 3),
            new RerankingProvider.Rank(3, 9),
            new RerankingProvider.Rank(4, 2),
            new RerankingProvider.Rank(5, 8),
            new RerankingProvider.Rank(6, 4));
    assertThat(response.modelUsage().promptTokens()).isEqualTo(7);
    assertThat(response.modelUsage().totalTokens()).isEqualTo(7);
    assertThat(response.modelUsage().batchCount()).isEqualTo(3);
    assertThat(response.modelUsage().tenant()).isEqualTo(TEST_CONSTANTS.TENANT);

    reranker.verify(
        3,
        postRequestedFor(urlEqualTo(RerankingTestResource.PATH))
            .withHeader("Authorization", equalTo("Bearer " + RerankingTestResource.API_KEY))
            .withHeader("tenant-id", equalTo(TEST_CONSTANTS.TENANT.toString())));
    var requests = reranker.findAll(postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
    assertThat(requests)
        .allSatisfy(
            request -> {
              var body = Json.node(request.getBodyAsString());
              assertThat(body.get("model").asText()).isEqualTo(MODEL);
              assertThat(body.get("query").get("text").asText()).isEqualTo("search query");
              assertThat(body.get("truncate").asText()).isEqualTo("NONE");
            });
    assertThat(
            requests.stream()
                .map(request -> Json.node(request.getBodyAsString()).get("passages").size())
                .toList())
        .containsExactlyInAnyOrder(3, 3, 1);
    assertThat(
            requests.stream()
                .flatMap(
                    request -> {
                      var texts = Json.node(request.getBodyAsString()).get("passages");
                      return IntStream.range(0, texts.size())
                          .mapToObj(index -> texts.get(index).get("text").asText());
                    })
                .toList())
        .containsExactlyInAnyOrderElementsOf(passages);
  }

  @Test
  void decodesNegativeAndFractionalLogits() {
    var response =
        provider(3)
            .rerank("query", List.of("score:-2.5 negative", "score:0.75 fractional"), CREDENTIALS)
            .await()
            .atMost(Duration.ofSeconds(10));

    assertThat(response.ranks())
        .containsExactly(
            new RerankingProvider.Rank(0, -2.5f), new RerankingProvider.Rank(1, 0.75f));
    reranker.verify(1, postRequestedFor(urlEqualTo(RerankingTestResource.PATH)));
  }

  private NvidiaRerankingProvider provider(int batchSize) {
    var properties =
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl
            .RequestPropertiesImpl(0, 10, 5000, 100, 0.5, batchSize);
    var model =
        new RerankingProvidersConfigImpl.RerankingProviderConfigImpl.ModelConfigImpl(
            MODEL,
            new ApiModelSupport.ApiModelSupportImpl(
                ApiModelSupport.SupportStatus.SUPPORTED, Optional.empty()),
            false,
            reranker.baseUrl() + RerankingTestResource.PATH,
            properties);
    return new NvidiaRerankingProvider(model);
  }
}
