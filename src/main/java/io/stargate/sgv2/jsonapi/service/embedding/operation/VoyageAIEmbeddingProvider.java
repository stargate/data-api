package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import io.stargate.sgv2.jsonapi.service.provider.ProviderHttpInterceptor;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class VoyageAIEmbeddingProvider extends EmbeddingProvider {

  private final VoyageAIEmbeddingProviderClient voyageClient;

  private final String requestTypeQuery, requestTypeIndex;
  private final Boolean autoTruncate;

  public VoyageAIEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.VERTEXAI,
        providerConfig,
        modelConfig,
        serviceConfig,
        dimension,
        vectorizeServiceParameters);

    // use configured input_type if available
    requestTypeQuery = requestProperties().requestTypeQuery().orElse(null);
    requestTypeIndex = requestProperties().requestTypeIndex().orElse(null);

    Object v =
        (vectorizeServiceParameters == null)
            ? null
            : vectorizeServiceParameters.get("autoTruncate");
    autoTruncate = (v instanceof Boolean) ? (Boolean) v : null;

    voyageClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(serviceConfig.getBaseUrl(modelName())))
            .readTimeout(requestProperties().readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(VoyageAIEmbeddingProviderClient.class);
  }

  /**
   * Response body with an error will look like below:
   *
   * <pre>
   * {"detail":"You have not yet added your payment method in the billing page and will have reduced rate limits of 3 RPM and 10K TPM.  Please add your payment method in the billing page (https://dash.voyageai.com/billing/payment-methods) to unlock our standard rate limits (https://docs.voyageai.com/docs/rate-limits).  Even with payment methods entered, the free tokens (50M tokens per model) will still apply."}
   *
   * {"detail":"Provided API key is invalid."}
   * </pre>
   */
  @Override
  protected String errorMessageJsonPtr() {
    return "/detail";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEmbeddingApiKeyHeader(embeddingCredentials.apiKey());

    // TODO: remove the requestTypeQuery and requestTypeIndex from config !
    // aaron 8 June 2025 - this looks like the term to sue for query and index is in config, but
    // there is
    // NOT handling of when this config is not set
    final String inputType =
        (embeddingRequestType == EmbeddingRequestType.SEARCH) ? requestTypeQuery : requestTypeIndex;

    var voyageRequest =
        new VoyageEmbeddingRequest(
            inputType, texts.toArray(new String[texts.size()]), modelName(), autoTruncate);

    // TODO: V2 error
    // aaron 8 June 2025 - old code had NO comment to explain what happens if the API key is empty.
    var accessToken = HttpConstants.BEARER_PREFIX_FOR_API_KEY + embeddingCredentials.apiKey().get();

    long callStartNano = System.nanoTime();
    return retryHTTPCall(voyageClient.embed(accessToken, voyageRequest))
        .onItem()
        .transform(
            jakartaResponse -> {
              var voyageResponse = jakartaResponse.readEntity(VoyageEmbeddingResponse.class);
              long callDurationNano = System.nanoTime() - callStartNano;

              // aaron - 10 June 2025 - previous code would silently swallow no data returned
              // and return an empty result. If we made a request we should get a response.
              if (voyageResponse.data() == null) {
                throw new IllegalStateException(
                    "ModelProvider %s returned empty data for model %s"
                        .formatted(modelProvider(), modelName()));
              }

              // TODO: WHY SORT ?
              Arrays.sort(voyageResponse.data(), (a, b) -> a.index() - b.index());

              List<float[]> vectors =
                  Arrays.stream(voyageResponse.data())
                      .map(VoyageEmbeddingResponse.Data::embedding)
                      .toList();

              var modelUsage =
                  createModelUsage(
                      embeddingCredentials.tenantId(),
                      ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                      0,
                      voyageResponse.usage.total_tokens,
                      jakartaResponse,
                      callDurationNano);
              return new BatchedEmbeddingResponse(batchId, vectors, modelUsage);
            });
  }

  /**
   * REST client interface for the Voyage Embedding Service.
   *
   * <p>..
   */
  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  @RegisterProvider(ProviderHttpInterceptor.class)
  public interface VoyageAIEmbeddingProviderClient {
    @POST
    // no path specified, as it is already included in the baseUri
    @ClientHeaderParam(name = HttpHeaders.CONTENT_TYPE, value = MediaType.APPLICATION_JSON)
    Uni<Response> embed(
        @HeaderParam("Authorization") String accessToken, VoyageEmbeddingRequest request);
  }

  /**
   * Request structure of the Voyage REST service.
   *
   * <p>..
   */
  public record VoyageEmbeddingRequest(
      @JsonInclude(JsonInclude.Include.NON_EMPTY) String input_type,
      String[] input,
      String model,
      @JsonInclude(JsonInclude.Include.NON_NULL) Boolean truncation) {}

  /**
   * Response structure of the Voyage REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public record VoyageEmbeddingResponse(Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    record Data(int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Usage(int total_tokens) {
      // Voyage API does not return prompt_tokens
    }
  }
}
