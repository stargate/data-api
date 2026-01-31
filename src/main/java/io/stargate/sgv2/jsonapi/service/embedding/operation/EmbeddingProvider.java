package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME;
import static jakarta.ws.rs.core.Response.Status.Family.CLIENT_ERROR;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.*;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.ServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.provider.*;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A provider for Embedding models , using {@link ModelType#EMBEDDING} */
public abstract class EmbeddingProvider extends ProviderBase {

  protected static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingProvider.class);

  // IMPORTANT: all of these config objects have some form of a request properties config,
  // use the one from the serviceConfig, as it should be the most specific for this
  // schema object. We should be able to remove ServiceConfig later - aaron 16 jue 2025
  // use {@link #requestProperties()} to access the request properties
  protected final EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig;
  protected final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig;
  protected final ServiceConfigStore.ServiceConfig serviceConfig;

  protected final int dimension;
  protected final Map<String, Object> vectorizeServiceParameters;

  protected final Duration initialBackOffDuration;
  protected final Duration maxBackOffDuration;

  protected EmbeddingProvider(
      ModelProvider modelProvider,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig modelConfig,
      ServiceConfigStore.ServiceConfig serviceConfig,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    super(modelProvider, ModelType.EMBEDDING);

    this.providerConfig = providerConfig;
    this.modelConfig = modelConfig;
    this.serviceConfig = serviceConfig;
    this.dimension = dimension;
    this.vectorizeServiceParameters = vectorizeServiceParameters;

    this.initialBackOffDuration = Duration.ofMillis(requestProperties().initialBackOffMillis());
    this.maxBackOffDuration = Duration.ofMillis(requestProperties().maxBackOffMillis());
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
    return requestProperties().maxBatchSize();
  }

  /**
   * Use this to get the properties for the request, including the URL , see comment at the top of
   * class
   */
  protected ServiceConfigStore.ServiceRequestProperties requestProperties() {
    return serviceConfig.requestProperties();
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
        throw ServerException.internalServerError(
            "Missing URL parameter '%s' (available: %s)".formatted(key, parameters.keySet()));
      }
      baseUrl.append(value);
    }
    return baseUrl.toString();
  }

  /** Check if the API key is present in the header */
  protected void checkEmbeddingApiKeyHeader(Optional<String> apiKey) {

    if (apiKey.isEmpty()) {
      throw EmbeddingProviderException.Code.EMBEDDING_PROVIDER_AUTHENTICATION_KEYS_NOT_PROVIDED.get(
          Map.of(
              "provider",
              modelProvider().apiName(),
              "message",
              "'%s' header is missing".formatted(EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME)));
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
    return requestProperties().jitter();
  }

  @Override
  protected int atMostRetries() {
    return requestProperties().atMostRetries();
  }

  @Override
  protected boolean decideRetry(Throwable throwable) {

    var retry =
        (throwable.getCause() instanceof APIException apiException
            && apiException.code.equals(
                EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT.name()));

    return retry || super.decideRetry(throwable);
  }

  /** Maps an HTTP response to a V1 JsonApiException */
  @Override
  protected RuntimeException mapHTTPError(Response jakartaResponse, String errorMessage) {

    if (jakartaResponse.getStatus() == Response.Status.REQUEST_TIMEOUT.getStatusCode()
        || jakartaResponse.getStatus() == Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {
      return EmbeddingProviderException.Code.EMBEDDING_PROVIDER_TIMEOUT.get(
          Map.of(
              "provider",
              modelProvider().apiName(),
              "httpStatus",
              String.valueOf(jakartaResponse.getStatus()),
              "errorMessage",
              errorMessage));
    }

    // Status code == 429
    if (jakartaResponse.getStatus() == Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
      return EmbeddingProviderException.Code.EMBEDDING_PROVIDER_RATE_LIMITED.get(
          Map.of(
              "provider",
              modelProvider().apiName(),
              "httpStatus",
              String.valueOf(jakartaResponse.getStatus()),
              "errorMessage",
              errorMessage));
    }

    // Status code in 4XX other than 429
    if (jakartaResponse.getStatusInfo().getFamily() == CLIENT_ERROR) {
      return EmbeddingProviderException.Code.EMBEDDING_PROVIDER_CLIENT_ERROR.get(
          "provider",
          modelProvider().apiName(),
          "httpStatus",
          String.valueOf(jakartaResponse.getStatus()),
          "errorMessage",
          errorMessage);
    }

    // Status code in 5XX
    if (jakartaResponse.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
      return EmbeddingProviderException.Code.EMBEDDING_PROVIDER_SERVER_ERROR.get(
          "provider",
          modelProvider().apiName(),
          "httpStatus",
          String.valueOf(jakartaResponse.getStatus()),
          "errorMessage",
          errorMessage);
    }

    // All other errors, Should never happen as all errors are covered above
    return EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.get(
        Map.of(
            "errorMessage",
            "Provider: %s; HTTP Status: %s; Error Message: %s"
                .formatted(modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage)));
  }

  /** Call this from the subclass when the response from the provider is empty */
  protected void throwEmptyData(Response jakartaResponse) {
    throw EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNEXPECTED_RESPONSE.get(
        Map.of(
            "errorMessage",
            "Provider: %s; HTTP Status: %s; Error Message: The embedding provider returned empty data for model %s"
                .formatted(modelProvider().apiName(), jakartaResponse.getStatus(), modelName())));
  }

  /**
   * Record to hold the batchId and embedding vectors
   *
   * @param batchId - Sequence number for the batch to order the vectors.
   * @param embeddings - Embedding vectors for the given text inputs.
   */
  public record BatchedEmbeddingResponse(
      int batchId, List<float[]> embeddings, ModelUsage modelUsage) implements Recordable {

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder
          .append("batchId", batchId)
          .append("embeddings", embeddings)
          .append("modelUsage", modelUsage);
    }
  }

  public enum EmbeddingRequestType {
    /** This is used when vectorizing data in write operation for indexing */
    INDEX,
    /** This is used when vectorizing data for search operation */
    SEARCH
  }
}
