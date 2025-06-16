package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;

// TODO: WRITE SOME DAM DOCUMENTATION !
@QuarkusTest
public class EmbeddingClientTestResource implements QuarkusTestResourceLifecycleManager {

  private WireMockServer wireMockServer;

  // NOTE: These are the host and path to use with this lifecycle manager.
  // previously the start() methods returned below to override quarkus config properties,
  //      return Map.of(
  //          "stargate.jsonapi.embedding.providers.nvidia.url",
  //          wireMockServer.baseUrl() + "/v1/embeddings",
  //          "stargate.jsonapi.embedding.providers.openai.url",
  //          wireMockServer.baseUrl() + "/v1/");

  public static final String HOST = "http://localhost:8080";
  public static final String NVIDIA_PATH = "/v1/embeddings";
  public static final String OPENAI_PATH = "/v1/";

  public static final String NVIDIA_URL = HOST + NVIDIA_PATH;
  public static final String OPENAI_URL = HOST + OPENAI_PATH;

  @Override
  public Map<String, String> start() {
    wireMockServer = new WireMockServer();
    wireMockServer.start();

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("429")))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody("{\"object\": \"list\"}")
                    .withStatus(429)
                    .withStatusMessage("Too Many Requests")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("400")))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody("{\"object\": \"list\"}")
                    .withStatus(400)
                    .withStatusMessage("Bad Request")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("503")))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody("{\"object\": \"list\"}")
                    .withStatus(503)
                    .withStatusMessage("Service Unavailable")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("408")))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody("{\"object\": \"list\"}")
                    .withStatus(408)
                    .withStatusMessage("Request Timeout")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("301")))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody("{\"object\": \"list\"}")
                    .withStatus(301)
                    .withStatusMessage("Moved Permanently")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing(MediaType.APPLICATION_JSON)))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
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
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, "application/xml")
                    .withBody("<object>list</object>")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("text/plain;charset=UTF-8")))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, "text/plain;charset=UTF-8")
                    .withBody("Not Found")
                    .withStatus(500)));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("no json body")))
            .willReturn(
                aResponse().withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withRequestBody(matchingJsonPath("$.input", containing("empty json body")))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody("{}")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withHeader("OpenAI-Organization", equalTo("org-id"))
            .withHeader("OpenAI-Project", equalTo("project-id"))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody(
                        """
                                    {
                             "object": "list",
                             "data": [
                               {
                                 "index": 0,
                                 "embedding": [
                                   -0.01,
                                   -0.01,
                                   0.01
                                 ],
                                 "object": "embedding"
                               }
                             ],
                             "model": "text-embedding-ada-002",
                             "usage": {
                               "prompt_tokens": 0,
                               "total_tokens": 0
                             }
                           }
                                    """)));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withHeader("OpenAI-Organization", equalTo("invalid org"))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody("{\"object\": \"list\"}")
                    .withStatus(401)
                    .withStatusMessage("Unauthorized")));

    wireMockServer.stubFor(
        post(urlEqualTo("/v1/embeddings"))
            .withHeader("OpenAI-Project", equalTo("invalid proj"))
            .willReturn(
                aResponse()
                    .withHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                    .withBody("{\"object\": \"list\"}")
                    .withStatus(401)
                    .withStatusMessage("Unauthorized")));

    return Map.of();
  }

  @Override
  public void stop() {
    if (null != wireMockServer) {
      wireMockServer.stop();
    }
  }
}
