package io.stargate.sgv2.jsonapi.testresource;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.Json;
import com.github.tomakehurst.wiremock.extension.ResponseTransformerV2;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Local HTTP reranker for integration tests using the production Nvidia client. Passages start with
 * {@code score:<number>} so their scores do not depend on retrieval or batch arrival order. Use
 * this resource with {@code restrictToAnnotatedClass = true} to isolate provider URLs.
 */
public class RerankingTestResource implements QuarkusTestResourceLifecycleManager {

  public static final String PATH = "/v1/ranking";
  public static final String API_KEY = "test-reranking-api-key";

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
            wireMockConfig().dynamicPort().extensions(new PassageScoreTransformer()));
    server.start();
    server.stubFor(
        post(urlEqualTo(PATH))
            .withHeader("Authorization", equalTo("Bearer " + API_KEY))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withTransformers(PassageScoreTransformer.NAME)));

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

  private static class PassageScoreTransformer implements ResponseTransformerV2 {
    private static final String NAME = "passage-scores";

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public boolean applyGlobally() {
      return false;
    }

    @Override
    public Response transform(Response response, ServeEvent event) {
      var request = Json.node(event.getRequest().getBodyAsString());
      var passages = request.get("passages");
      var rankings = new ArrayList<Map<String, Object>>(passages.size());
      for (int index = 0; index < passages.size(); index++) {
        var passage = passages.get(index).get("text").asText();
        if (!passage.startsWith("score:")) {
          throw new IllegalArgumentException("Test passage must start with score:<number>");
        }
        float score = Float.parseFloat(passage.substring(6).split("\\s+", 2)[0]);
        rankings.add(Map.of("index", index, "logit", score));
      }
      // Provider order differs from passage order, so clients must honor each local index.
      rankings.sort(
          (left, right) -> Float.compare((Float) right.get("logit"), (Float) left.get("logit")));
      var body =
          Map.of(
              "rankings",
              rankings,
              "usage",
              Map.of("prompt_tokens", passages.size(), "total_tokens", passages.size()));
      return Response.Builder.like(response).but().body(Json.write(body)).build();
    }
  }
}
