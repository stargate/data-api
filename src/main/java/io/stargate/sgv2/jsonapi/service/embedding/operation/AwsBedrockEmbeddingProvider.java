package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME;
import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ProviderConstants;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

/** Provider implementation for AWS Bedrock. To start we support only Titan embedding models. */
public class AwsBedrockEmbeddingProvider extends EmbeddingProvider {

  private static final String providerId = ProviderConstants.BEDROCK;
  private static final ObjectWriter ow = new ObjectMapper().writer();
  private static final ObjectReader or = new ObjectMapper().reader();

  public AwsBedrockEmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    super(
        requestProperties,
        baseUrl,
        model,
        acceptsTitanAIDimensions(model.name()) ? dimension : 0,
        vectorizeServiceParameters,
        providerConfig);
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {
    // Check if using an EOF model
    checkEOLModelUsage();
    if (embeddingCredentials.accessId().isEmpty() && embeddingCredentials.secretId().isEmpty()) {
      throw ErrorCodeV1.EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.toApiException(
          "Both '%s' and '%s' are missing in the header for provider '%s'",
          EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME,
          EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME,
          providerId);
    } else if (embeddingCredentials.accessId().isEmpty()) {
      throw ErrorCodeV1.EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.toApiException(
          "'%s' is missing in the header for provider '%s'",
          EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME, providerId);
    } else if (embeddingCredentials.secretId().isEmpty()) {
      throw ErrorCodeV1.EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.toApiException(
          "'%s' is missing in the header for provider '%s'",
          EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME, providerId);
    }

    AwsBasicCredentials awsCreds =
        AwsBasicCredentials.create(
            embeddingCredentials.accessId().get(), embeddingCredentials.secretId().get());

    BedrockRuntimeAsyncClient client =
        BedrockRuntimeAsyncClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
            .region(Region.of(vectorizeServiceParameters.get("region").toString()))
            .build();
    final CompletableFuture<InvokeModelResponse> invokeModelResponseCompletableFuture =
        client.invokeModel(
            request -> {
              final byte[] inputData;
              try {
                inputData = ow.writeValueAsBytes(new EmbeddingRequest(texts.get(0), dimension));
                request.body(SdkBytes.fromByteArray(inputData)).modelId(model.name());
              } catch (JsonProcessingException e) {
                throw ErrorCodeV1.EMBEDDING_REQUEST_ENCODING_ERROR.toApiException();
              }
            });

    final CompletableFuture<Response> responseCompletableFuture =
        invokeModelResponseCompletableFuture.thenApply(
            res -> {
              try {
                EmbeddingResponse response =
                    or.readValue(res.body().asInputStream(), EmbeddingResponse.class);
                List<float[]> vectors = List.of(response.embedding);
                return Response.of(batchId, vectors);
              } catch (IOException e) {
                throw ErrorCodeV1.EMBEDDING_RESPONSE_DECODING_ERROR.toApiException();
              }
            });

    return Uni.createFrom()
        .completionStage(responseCompletableFuture)
        .onFailure(BedrockRuntimeException.class)
        .transform(
            error -> {
              BedrockRuntimeException bedrockRuntimeException = (BedrockRuntimeException) error;
              // Status code == 408 and 504 for timeout
              if (bedrockRuntimeException.statusCode()
                      == jakarta.ws.rs.core.Response.Status.REQUEST_TIMEOUT.getStatusCode()
                  || bedrockRuntimeException.statusCode()
                      == jakarta.ws.rs.core.Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {
                return ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT.toApiException(
                    "Provider: %s; HTTP Status: %s; Error Message: %s",
                    providerId,
                    bedrockRuntimeException.statusCode(),
                    bedrockRuntimeException.getMessage());
              }

              // Status code == 429
              if (bedrockRuntimeException.statusCode()
                  == jakarta.ws.rs.core.Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
                return ErrorCodeV1.EMBEDDING_PROVIDER_RATE_LIMITED.toApiException(
                    "Provider: %s; HTTP Status: %s; Error Message: %s",
                    providerId,
                    bedrockRuntimeException.statusCode(),
                    bedrockRuntimeException.getMessage());
              }

              // Status code in 4XX other than 429
              if (bedrockRuntimeException.statusCode() > 400
                  && bedrockRuntimeException.statusCode() < 500) {
                return ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR.toApiException(
                    "Provider: %s; HTTP Status: %s; Error Message: %s",
                    providerId,
                    bedrockRuntimeException.statusCode(),
                    bedrockRuntimeException.getMessage());
              }

              // Status code in 5XX
              if (bedrockRuntimeException.statusCode() >= 500) {
                return ErrorCodeV1.EMBEDDING_PROVIDER_SERVER_ERROR.toApiException(
                    "Provider: %s; HTTP Status: %s; Error Message: %s",
                    providerId,
                    bedrockRuntimeException.statusCode(),
                    bedrockRuntimeException.getMessage());
              }

              // All other errors, Should never happen as all errors are covered above
              return ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
                  "Provider: %s; HTTP Status: %s; Error Message: %s",
                  providerId,
                  bedrockRuntimeException.statusCode(),
                  bedrockRuntimeException.getMessage());
            });
  }

  private record EmbeddingRequest(
      String inputText, @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions) {}

  @JsonIgnoreProperties(ignoreUnknown = true) // ignore possible extra fields without error
  private record EmbeddingResponse(float[] embedding, int inputTextTokenCount) {}

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
