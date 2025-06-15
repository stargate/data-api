package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME;
import static io.stargate.sgv2.jsonapi.exception.ErrorCodeV1.EMBEDDING_PROVIDER_API_KEY_MISSING;
import static jakarta.ws.rs.core.Response.Status.Family.CLIENT_ERROR;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.provider.*;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TODO */
public abstract class EmbeddingProvider extends ProviderBase {

  protected static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingProvider.class);

  protected final EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig;
  protected final String baseUrl;
  protected final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig;
  protected final int dimension;
  protected final Map<String, Object> vectorizeServiceParameters;


  protected final Duration initialBackOffDuration;
  protected final Duration maxBackOffDuration;

  protected EmbeddingProvider(
      ModelProvider modelProvider,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(modelProvider, ModelType.EMBEDDING);

    this.providerConfig = providerConfig;
    this.baseUrl = baseUrl;
    this.modelConfig = modelConfig;
    this.dimension = dimension;
    this.vectorizeServiceParameters = vectorizeServiceParameters;


    this.initialBackOffDuration = Duration.ofMillis(providerConfig.properties().initialBackOffMillis());
    this.maxBackOffDuration = Duration.ofMillis(providerConfig.properties().maxBackOffMillis());
  }

  @Override
  public String modelName() {
    return modelConfig.name();
  }

  @Override
  public ApiModelSupport modelSupport() {
    return modelConfig.apiModelSupport();
  }

  public EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig() {
    return providerConfig;
  }

  /**
   * Vectorizes the given list of texts and returns the embeddings.
   *
   * @param texts List of texts to be vectorized
   * @param embeddingCredentials embeddingCredentials required for the provider
   * @param embeddingRequestType Type of request (INDEX or SEARCH)
   * @return VectorResponse
   */
  public abstract Uni<BatchedEmbeddingResponse> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType);

  /**
   * returns the maximum batch size supported by the provider
   *
   * @return
   */
  public int maxBatchSize() {
    return providerConfig.properties().maxBatchSize();
  }

  /**
   * Helper method that has logic wrt whether OpenAI (azure or regular) accepts {@code "dimensions"}
   * parameter or not.
   *
   * @param modelName OpenAI model to check
   * @return True if given OpenAI model accepts (and expects} {@code "dimensions"} parameter; false
   *     if not.
   */
  protected static boolean acceptsOpenAIDimensions(String modelName) {
    return !modelName.endsWith("-ada-002");
  }

  /**
   * Helper method that has logic wrt whether JinaAI accepts {@code "dimensions"} parameter or not.
   *
   * @param modelName JinaAI model to check
   * @return True if given JinaAI model accepts (and expects} {@code "dimensions"} parameter; false
   *     if not.
   */
  protected static boolean acceptsJinaAIDimensions(String modelName) {
    return modelName.endsWith("jina-embeddings-v3");
  }

  /**
   * Helper method that has logic wrt whether AWS Titan accepts {@code "dimensions"} parameter or
   * not.
   *
   * @param modelName Amazon Titan model to check
   * @return True if given Amazon Titan model accepts (and expects} {@code "dimensions"} parameter;
   *     false if not.
   */
  protected static boolean acceptsTitanAIDimensions(String modelName) {
    return modelName.endsWith("titan-embed-text-v2:0");
  }

  /**
   * Replace parameters in a messageTemplate string with values from a map: placeholders are of form
   * {@code {parameterName}} and matching value to look for in the map is String {@code
   * "parameterName"}.
   *
   * @param template Template with placeholders to replace
   * @param parameters Parameters to replace in the messageTemplate
   * @return Processed messageTemplate with replaced parameters
   */
  protected String replaceParameters(String template, Map<String, Object> parameters) {
    final int len = template.length();
    StringBuilder baseUrl = new StringBuilder(len);

    for (int i = 0; i < len; ) {
      char c = template.charAt(i++);
      int end;

      if ((c != '{') || (end = template.indexOf('}', i)) < 0) {
        baseUrl.append(c);
        continue;
      }
      String key = template.substring(i, end);
      i = end + 1;

      Object value = parameters.get(key);
      if (value == null) {
        throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
            "Missing URL parameter '%s' (available: %s)", key, parameters.keySet());
      }
      baseUrl.append(value);
    }
    return baseUrl.toString();
  }

  /** Check if the API key is present in the header */
  protected void checkEmbeddingApiKeyHeader(Optional<String> apiKey) {

    if (apiKey.isEmpty()) {
      throw EMBEDDING_PROVIDER_API_KEY_MISSING.toApiException(
          "header value `%s` is missing for embedding provider: %s",
          EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME, modelProvider().apiName());
    }
  }


  @Override
  protected Duration initialBackOffDuration() {
    return initialBackOffDuration;
  }

  @Override
  protected Duration maxBackOffDuration() {
    return maxBackOffDuration;
  }

  @Override
  protected double jitter() {
    return  providerConfig.properties().jitter();
  }

  @Override
  protected int atMostRetries() {
    return providerConfig.properties().atMostRetries();
  }

  @Override
  protected boolean decideRetry(Throwable throwable) {

    var retry =
        (throwable.getCause() instanceof JsonApiException jae
            && jae.getErrorCode() == ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT);

    return retry || super.decideRetry(throwable);
  }

  /** Maps an HTTP response to a V1 JsonApiException */
  @Override
  protected RuntimeException mapHTTPError(Response jakartaResponse, String errorMessage) {

    if (jakartaResponse.getStatus() == Response.Status.REQUEST_TIMEOUT.getStatusCode()
        || jakartaResponse.getStatus() == Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage);
    }

    // Status code == 429
    if (jakartaResponse.getStatus() == Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_RATE_LIMITED.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage);
    }

    // Status code in 4XX other than 429
    if (jakartaResponse.getStatusInfo().getFamily() == CLIENT_ERROR) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_CLIENT_ERROR.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage);
    }

    // Status code in 5XX
    if (jakartaResponse.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
      return ErrorCodeV1.EMBEDDING_PROVIDER_SERVER_ERROR.toApiException(
          "Provider: %s; HTTP Status: %s; Error Message: %s",
          modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage);
    }

    // All other errors, Should never happen as all errors are covered above
    return ErrorCodeV1.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.toApiException(
        "Provider: %s; HTTP Status: %s; Error Message: %s",
        jakartaResponse, jakartaResponse.getStatus(), errorMessage);
  }

  /**
   * Record to hold the batchId and embedding vectors
   *
   * @param batchId - Sequence number for the batch to order the vectors.
   * @param embeddings - Embedding vectors for the given text inputs.
   */
  public record BatchedEmbeddingResponse(
      int batchId, List<float[]> embeddings, ModelUsage modelUsage) implements Recordable {

    public static BatchedEmbeddingResponse empty(int batchId) {
      return new BatchedEmbeddingResponse(batchId, List.of(), ModelUsage.EMPTY);
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("batchId", batchId)
          .append("embeddings", embeddings)
          .append("modelUsage", modelUsage);
    }
  }

  // TODO: remove and use the general ModelInputType enum
  public enum EmbeddingRequestType {
    /** This is used when vectorizing data in write operation for indexing */
    INDEX,
    /** This is used when vectorizing data for search operation */
    SEARCH
  }
}
