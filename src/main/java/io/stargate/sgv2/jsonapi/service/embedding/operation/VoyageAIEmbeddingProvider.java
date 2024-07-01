package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderResponseValidation;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.error.HttpResponseErrorMessageMapper;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

public class VoyageAIEmbeddingProvider extends EmbeddingProvider {
  private static final String providerId = ProviderConstants.VOYAGE_AI;
  private final VoyageAIEmbeddingProviderClient voyageAIEmbeddingProviderClient;
  private final String requestTypeQuery, requestTypeIndex;
  private final Boolean autoTruncate;

  public VoyageAIEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> serviceParameters) {
    super(requestProperties, baseUrl, modelName, dimension, serviceParameters);

    // use configured input_type if available
    requestTypeQuery = requestProperties.requestTypeQuery().orElse(null);
    requestTypeIndex = requestProperties.requestTypeIndex().orElse(null);
    Object v = (serviceParameters == null) ? null : serviceParameters.get("autoTruncate");
    autoTruncate = (v instanceof Boolean) ? (Boolean) v : null;

    voyageAIEmbeddingProviderClient =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .readTimeout(requestProperties.readTimeoutMillis(), TimeUnit.MILLISECONDS)
            .build(VoyageAIEmbeddingProviderClient.class);
  }

  @RegisterRestClient
  @RegisterProvider(EmbeddingProviderResponseValidation.class)
  public interface VoyageAIEmbeddingProviderClient {
    @POST
    // no path specified, as it is already included in the baseUri
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);

    @ClientExceptionMapper
    static RuntimeException mapException(jakarta.ws.rs.core.Response response) {
      String errorMessage = getErrorMessage(response);
      return HttpResponseErrorMessageMapper.mapToAPIException(providerId, response, errorMessage);
    }

    /**
     * Extract the error message from the response body. The example response body is:
     *
     * <pre>
     * {"detail":"You have not yet added your payment method in the billing page and will have reduced rate limits of 3 RPM and 10K TPM.  Please add your payment method in the billing page (https://dash.voyageai.com/billing/payment-methods) to unlock our standard rate limits (https://docs.voyageai.com/docs/rate-limits).  Even with payment methods entered, the free tokens (50M tokens per model) will still apply."}
     *
     * {"detail":"Provided API key is invalid."}
     * </pre>
     *
     * @param response The response body as a String.
     * @return The error message extracted from the response body.
     */
    private static String getErrorMessage(jakarta.ws.rs.core.Response response) {
      // Get the whole response body
      JsonNode rootNode = response.readEntity(JsonNode.class);
      // Log the response body
      logger.info(
          String.format(
              "Error response from embedding provider '%s': %s", providerId, rootNode.toString()));
      // Extract the "detail" node
      JsonNode detailNode = rootNode.path("detail");
      // Return the text of the "detail" node, or the full response body if it is missing
      return detailNode.isMissingNode() ? rootNode.toString() : detailNode.toString();
    }
  }

  record EmbeddingRequest(
      @JsonInclude(JsonInclude.Include.NON_EMPTY) String input_type,
      String[] input,
      String model,
      @JsonInclude(JsonInclude.Include.NON_NULL) Boolean truncation) {}

  @JsonIgnoreProperties({"object"})
  record EmbeddingResponse(Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties({"object"})
    record Data(int index, float[] embedding) {}

    record Usage(int total_tokens) {}
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Credentials credentials,
      EmbeddingRequestType embeddingRequestType) {
    final String inputType =
        (embeddingRequestType == EmbeddingRequestType.SEARCH) ? requestTypeQuery : requestTypeIndex;
    String[] textArray = new String[texts.size()];
    EmbeddingRequest request =
        new EmbeddingRequest(inputType, texts.toArray(textArray), modelName, autoTruncate);

    Uni<EmbeddingResponse> response =
        applyRetry(
            voyageAIEmbeddingProviderClient.embed("Bearer " + credentials.apiKey().get(), request));

    return response
        .onItem()
        .transform(
            resp -> {
              if (resp.data() == null) {
                return Response.of(batchId, Collections.emptyList());
              }
              Arrays.sort(resp.data(), (a, b) -> a.index() - b.index());
              List<float[]> vectors =
                  Arrays.stream(resp.data()).map(data -> data.embedding()).toList();
              return Response.of(batchId, vectors);
            });
  }

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
