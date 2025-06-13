package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ProviderHttpInterceptor;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of client that talks to Azure-deployed OpenAI embedding provider. See <a
 * href="https://learn.microsoft.com/en-us/azure/ai-services/openai/reference">API reference</a> for
 * details of REST API being called.
 */
public class AzureOpenAIEmbeddingProvider extends EmbeddingProvider {

  private final AzureOpenAIEmbeddingProviderClient azureClient;

  public AzureOpenAIEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    // One special case: legacy "ada-002" model does not accept "dimension" parameter
    super(
        ModelProvider.AZURE_OPENAI,
        providerConfig,
        baseUrl,
        modelConfig,
        acceptsOpenAIDimensions(modelConfig.name()) ? dimension : 0,
        vectorizeServiceParameters);

    String actualUrl = replaceParameters(baseUrl, vectorizeServiceParameters);
    azureClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(actualUrl))
            .readTimeout(providerConfig.properties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(AzureOpenAIEmbeddingProviderClient.class);
  }

  /**
   * The example response body is:
   *
   * <pre>
   * {
   *   "error": {
   *     "code": "401",
   *     "message": "Access denied due to invalid subscription key or wrong API endpoint. Make sure to provide a valid key for an active subscription and use a correct regional API endpoint for your resource."
   *   }
   * }
   * </pre>
   */
  @Override
  protected String errorMessageJsonPtr() {
    return "/error/message";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEOLModelUsage();
    checkEmbeddingApiKeyHeader(embeddingCredentials.apiKey());
    var azureRequest =
        new AzureOpenAIEmbeddingRequest(
            texts.toArray(new String[texts.size()]), modelName(), dimension);

    // TODO: V2 error
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    // NOTE: NO "Bearer " prefix with API key for Azure
    var accessToken = embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();
    return retryHTTPCall(azureClient.embed(accessToken, azureRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var azureResponse = jakartaResponse.readEntity(AzureOpenAIEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (azureResponse.data() == null) {
                throw new IllegalStateException(
                    "ModelProvider %s returned empty data for model %s"
                        .formatted(modelProvider(), modelName()));
              }

              Arrays.sort(azureResponse.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(azureResponse.data())
                      .map(AzureOpenAIEmbeddingResponse.Data::embedding)
                      .toList();

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenantId(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      azureResponse.usage().prompt_tokens(),
                      azureResponse.usage().total_tokens(),
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the Azure Open AI Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface
  AzureOpenAIEmbeddingProviderClient {
    // no path specified, as it is already included in the baseUri
    @POST
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        // API keys as "api-key", MS Entra as "Authorization: Bearer [token]
        @HeaderParam("api-key") String accessToken, AzureOpenAIEmbeddingRequest request);
  }

  /**
   * Request structure of the Azure Open AI REST service.
   *
   * <p>..
   */
  public record AzureOpenAIEmbeddingRequest(
      String[] input,
      String model,
      @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions) {}

  /**
   * Response structure of the Azure Open AI REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record AzureOpenAIEmbeddingResponse(
      String object, Data[] data, String model, Usage usage) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Data(String object, int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }
}
