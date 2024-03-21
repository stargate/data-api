package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import java.util.Map;

@QuarkusTest
public class EmbeddingClientTestResource implements QuarkusTestResourceLifecycleManager {

  private WireMockServer wireMockServer;

  @Override
  public Map<String, String> start() {
    wireMockServer = new WireMockServer();
    wireMockServer.start();

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("429")))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withStatus(429)
                    .withStatusMessage("Too Many Requests")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("400")))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withStatus(400)
                    .withStatusMessage("Bad Request")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("503")))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withStatus(503)
                    .withStatusMessage("Service Unavailable")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("408")))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withStatus(408)
                    .withStatusMessage("Request Timeout")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("301")))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withStatus(301)
                    .withStatusMessage("Moved Permanently")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("application/json")))
            .willReturn(
                aResponse()
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                           {
                             "object": "list",
                             "data": [
                               {
                                 "index": 0,
                                 "embedding": [
                                   -0.0175628662109375,
                                   -0.035247802734375,
                                   0.044586181640625
                                 ],
                                 "object": "embedding"
                               }
                             ],
                             "model": "NV-Embed-QA",
                             "usage": {
                               "prompt_tokens": 0,
                               "total_tokens": 0
                             }
                           }
                                 """)));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("application/xml")))
            .willReturn(aResponse().withHeader("Content-Type", "application/xml").withBody("{}")));
    return Map.of(
        "stargate.jsonapi.embedding.providers.nvidia.url",
        wireMockServer.baseUrl() + "/v1/embeddings");
  }

  @Override
  public void stop() {
    if (null != wireMockServer) {
      wireMockServer.stop();
    }
  }
}
