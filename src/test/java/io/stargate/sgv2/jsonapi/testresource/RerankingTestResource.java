package io.stargate.sgv2.jsonapi.testresource;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.Options.ChunkedEncodingPolicy;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Local HTTP reranker for integration tests using the production Nvidia client. Passages start with
 * {@code score:<number>} so their scores do not depend on retrieval or batch arrival order. Use
 * this resource with {@code restrictToAnnotatedClass = true} to isolate provider URLs.
 *
 * <p>{@link #startFakeReranker} starts the same fake for {@code FakeRerankerTestResource}, with one
 * path per model and the special replies of {@link FakeRerankTransformer}.
 */
public class RerankingTestResource implements QuarkusTestResourceLifecycleManager {

  public static final String PATH = "/v1/ranking";
  public static final String API_KEY = "test-reranking-api-key";

  /** Path prefix of the model paths of {@link #startFakeReranker}. */
  public static final String RERANK_PATH = "/rerank/";

  /** The path that the {@code REDIRECT} reply of {@link FakeRerankTransformer} points to. */
  public static final String REDIRECT_TARGET = RERANK_PATH + "redirect-target";

  private WireMockServer server;

  @Override
  public Map<String, String> start() {
    Map<String, String> config;
    try {
      var configResource =
          Objects.requireNonNull(
              getClass().getClassLoader().getResource("test-reranking-providers-config.yaml"),
              "Test reranking provider config must be available");
      // Resource selection uses system properties before Quarkus applies test overrides. Supply
      // the complete test configuration so the fixture also works with the one-model default.
      config = new HashMap<>(new YamlConfigSource(configResource).getProperties());
    } catch (IOException e) {
      throw new IllegalStateException("Cannot load test reranking provider config", e);
    }

    server =
        new WireMockServer(
            wireMockConfig().dynamicPort().extensions(new FakeRerankTransformer(null, 1)));
    server.start();
    server.stubFor(
        post(urlEqualTo(PATH))
            .withHeader("Authorization", equalTo("Bearer " + API_KEY))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withTransformers(FakeRerankTransformer.NAME)));

    config.put("stargate.jsonapi.operations.enable-embedding-gateway", "false");
    // All three Nvidia entries in test-reranking-providers-config.yaml use the local server.
    for (int model = 0; model < 3; model++) {
      config.put(
          "stargate.jsonapi.reranking.providers.nvidia.models[%s].url".formatted(model),
          server.baseUrl() + PATH);
    }
    return config;
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(server, new TestInjector.MatchesType(WireMockServer.class));
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop();
    }
  }

  /**
   * Starts a fake reranker on 127.0.0.1 and a random port, without chunked or gzip replies. Every
   * request under {@link #RERANK_PATH} gets a reply of {@link FakeRerankTransformer}, which checks
   * it against {@code modelsByPath}; {@link #REDIRECT_TARGET} replies {@code {"redirected":true}}
   * to anything. Tests add stubs with a higher priority for special replies.
   */
  public static WireMockServer startFakeReranker(Map<String, String> modelsByPath) {
    var server =
        new WireMockServer(
            wireMockConfig()
                .dynamicPort()
                .bindAddress("127.0.0.1")
                .useChunkedTransferEncoding(ChunkedEncodingPolicy.NEVER)
                .gzipDisabled(true)
                .extensions(new FakeRerankTransformer(modelsByPath, 10)));
    server.start();
    server.stubFor(
        any(urlPathMatching(RERANK_PATH + ".+"))
            .atPriority(9)
            .willReturn(aResponse().withTransformers(FakeRerankTransformer.NAME)));
    server.stubFor(
        any(urlPathEqualTo(REDIRECT_TARGET))
            .atPriority(1)
            .willReturn(okJson("{\"redirected\":true}")));
    return server;
  }
}
