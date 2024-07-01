package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeAsyncClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

/** Provider implementation for AWS Bedrock. To start we support only Titan embedding models. */
public class AwsBedrockEnbeddingProvider extends EmbeddingProvider {
  private static ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
  private static ObjectReader or = new ObjectMapper().reader();

  public AwsBedrockEnbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(
        requestProperties,
        baseUrl,
        modelName,
        acceptsTitanAIDimensions(modelName) ? dimension : 0,
        vectorizeServiceParameters);
  }

  @Override
  public Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingProvider.Credentials credentials,
      EmbeddingRequestType embeddingRequestType) {
    if (!credentials.accessKeyId().isPresent() || !credentials.secretAccessKey().isPresent()) {
      throw new RuntimeException("AccessId and SecretId are required for AWS Bedrock");
    }
    AwsBasicCredentials awsCreds =
        AwsBasicCredentials.create(
            credentials.accessKeyId().get(), credentials.secretAccessKey().get());

    BedrockRuntimeAsyncClient client =
        BedrockRuntimeAsyncClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
            .region(Region.of(vectorizeServiceParameters.get("region").toString()))
            .build();
    final CompletableFuture<InvokeModelResponse> invokeModelResponseCompletableFuture =
        client.invokeModel(
            request -> {
              try {
                String inputJson =
                    ow.writeValueAsString(new EmbeddingRequest(texts.get(0), dimension));
                request.body(SdkBytes.fromUtf8String(inputJson)).modelId(modelName);
              } catch (SdkClientException e) {
                throw new RuntimeException(e);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
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
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    return Uni.createFrom().completionStage(responseCompletableFuture);
  }

  private record EmbeddingRequest(
      String inputText, @JsonInclude(value = JsonInclude.Include.NON_DEFAULT) int dimensions) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record EmbeddingResponse(float[] embedding, int inputTextTokenCount) {}

  @Override
  public int maxBatchSize() {
    return requestProperties.maxBatchSize();
  }
}
