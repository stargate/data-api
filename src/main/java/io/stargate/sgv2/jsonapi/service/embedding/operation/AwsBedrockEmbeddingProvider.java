package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME;
import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.io.CountingOutputStream;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.ModelInputType;
import io.stargate.sgv2.jsonapi.service.provider.ModelProvider;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;

/** Provider implementation for AWS Bedrock. To start we support only Titan embedding models. */
public class AwsBedrockEmbeddingProvider extends EmbeddingProvider {

  private static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer();
  private static final ObjectReader OBJECT_READER = new ObjectMapper().reader();

  public AwsBedrockEmbeddingProvider(
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        ModelProvider.BEDROCK,
        providerConfig,
        modelConfig,
        serviceConfig,
        acceptsTitanAIDimensions(modelConfig.name()) ? dimension : 0,
        vectorizeServiceParameters);
  }

  @Override
  protected String errorMessageJsonPtr() {
    // not used in this provider, has custom error handling
    return "";
  }

  @Override
  public Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType) {

    checkEOLModelUsage();

    // the config should mean we only do a batch on 1, sanity checking
    if (texts.size() != 1) {
      throw new IllegalArgumentException(
          "AWS Bedrock embedding provider only supports a single text input per request, but received: "
              + texts.size());
    }

    // TODO: move to V2 errors
    if (embeddingCredentials.accessId().isEmpty() && embeddingCredentials.secretId().isEmpty()) {
      throw ErrorCodeV1.EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.toApiException(
          "Both '%s' and '%s' are missing in the header for provider '%s'",
          EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME,
          EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME,
          modelProvider().apiName());
    } else if (embeddingCredentials.accessId().isEmpty()) {
      throw ErrorCodeV1.EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.toApiException(
          "'%s' is missing in the header for provider '%s'",
          EMBEDDING_AUTHENTICATION_ACCESS_ID_HEADER_NAME, modelProvider().apiName());
    } else if (embeddingCredentials.secretId().isEmpty()) {
      throw ErrorCodeV1.EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.toApiException(
          "'%s' is missing in the header for provider '%s'",
          EMBEDDING_AUTHENTICATION_SECRET_ID_HEADER_NAME, modelProvider().apiName());
    }

    var awsCreds =
        AwsBasicCredentials.create(
            embeddingCredentials.accessId().get(), embeddingCredentials.secretId().get());

    // NOTE: cannot put this client in a resource block for auto close because it will close
    // te connection pool before we pull the async result.
    var bedrockClient =
        BedrockRuntimeAsyncClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
            .region(Region.of(vectorizeServiceParameters.get("region").toString()))
            .build();

    long callStartNano = System.nanoTime();

    // NOTE: need to use the AWS client for the request, not a Rest Easy, so we cannot use
    // all the features from the superclasses such as error mapping and building the model usage
    var bytesUsageTracker = new ByteUsageTracker();
    var bedrockFuture =
        bedrockClient
            .invokeModel(
                requestBuilder -> {
                  byte[] inputData;
                  try {
                    inputData =
                        OBJECT_WRITER.writeValueAsBytes(
                            new AwsBedrockEmbeddingRequest(texts.getFirst(), dimension));
                  } catch (JacksonException e) { // should never happen
                    throw ErrorCodeV1.EMBEDDING_REQUEST_ENCODING_ERROR.toApiException();
                  }
                  bytesUsageTracker.requestBytes = inputData.length;
                  requestBuilder.body(SdkBytes.fromByteArray(inputData)).modelId(modelName());
                })
            .thenApply(
                rawResponse -> {
                  try {
                    // aws docs say do not need to close the stream
                    var inputStream = rawResponse.body().asInputStream();
                    var bedrockResponse =
                        OBJECT_READER.readValue(inputStream, AwsBedrockEmbeddingResponse.class);
                    long callDurationNano = System.nanoTime() - callStartNano;

                    try (var countingOut =
                        new CountingOutputStream(OutputStream.nullOutputStream())) {
                      inputStream.transferTo(countingOut);
                      long responseSize = countingOut.getCount();
                      bytesUsageTracker.responseBytes =
                          responseSize > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) responseSize;
                    }

                    var modelUsage =
                        createModelUsage(
                            embeddingCredentials.tenant(),
                            ModelInputType.fromEmbeddingRequestType(embeddingRequestType),
                            bedrockResponse.inputTextTokenCount(),
                            bedrockResponse.inputTextTokenCount(),
                            bytesUsageTracker.requestBytes,
                            bytesUsageTracker.responseBytes,
                            callDurationNano);

                    return new BatchedEmbeddingResponse(
                        batchId, List.of(bedrockResponse.embedding), modelUsage);

                  } catch (IOException e) {
                    throw ErrorCodeV1.EMBEDDING_RESPONSE_DECODING_ERROR.toApiException();
                  }
                });

    return Uni.createFrom()
        .completionStage(bedrockFuture)
        .onFailure(BedrockRuntimeException.class)
        .transform(throwable -> mapBedrockException((BedrockRuntimeException) throwable));
  }

  private Throwable mapBedrockException(BedrockRuntimeException bedrockException) {

    if (bedrockException.statusCode() == Response.Status.REQUEST_TIMEOUT.getStatusCode()
        || bedrockException.statusCode() == Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          modelProvider().apiName(), bedrockException.statusCode(), bedrockException.getMessage());
    }

    if (bedrockException.statusCode() == Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_RATE_LIMITED.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          modelProvider().apiName(), bedrockException.statusCode(), bedrockException.getMessage());
    }

    if (bedrockException.statusCode() > 400 && bedrockException.statusCode() < 500) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          modelProvider().apiName(), bedrockException.statusCode(), bedrockException.getMessage());
    }

    if (bedrockException.statusCode() >= 500) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_SERVER_ERROR.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          modelProvider().apiName(), bedrockException.statusCode(), bedrockException.getMessage());
    }

    // All other errors, Should never happen as all errors are covered above
    return ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
        "Provider: %s; HTTP Status: %s; Error Message: %s",
        modelProvider().apiName(), bedrockException.statusCode(), bedrockException.getMessage());
  }

  private static class ByteUsageTracker {
    int requestBytes = 0;
    int responseBytes = 0;
  }

  /**
   * Request structure of the AWS Bedrock REST service.
   *
   * <p>..
   */
  public record AwsBedrockEmbeddingRequest(
      String inputText, @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions) {}

  /**
   * Response structure of the AWS Bedrock REST service.
   *
   * <p>..
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  private record AwsBedrockEmbeddingResponse(float[] embedding, int inputTextTokenCount) {}
}
