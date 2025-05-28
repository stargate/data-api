package io.stargate.sgv2.jsonapi.service.embedding.operation;

import static io.stargate.sgv2.jsonapi.config.constants.HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME;
import static io.stargate.sgv2.jsonapi.exception.ErrorCodeV1.EMBEDDING_PROVIDER_API_KEY_MISSING;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProvidersConfig;
import io.stargate.sgv2.jsonapi.service.provider.ApiModelSupport;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen model.
 */
public abstract class EmbeddingProvider {
  protected static final Logger logger = LoggerFactory.getLogger(EmbeddingProvider.class);
  protected final EmbeddingProviderConfigStore.RequestProperties requestProperties;
  protected final String baseUrl;
  protected final EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model;
  protected final int dimension;
  protected final Map<String, Object> vectorizeServiceParameters;
  protected final EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig;

  /** Default constructor */
  protected EmbeddingProvider() {
    this(null, null, null, 0, null, null);
  }

  /** Constructs an EmbeddingProvider with the specified configuration. */
  protected EmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      EmbeddingProvidersConfig.EmbeddingProviderConfig.ModelConfig model,
      int dimension,
      Map<String, Object> vectorizeServiceParameters,
      EmbeddingProvidersConfig.EmbeddingProviderConfig providerConfig) {
    this.requestProperties = requestProperties;
    this.baseUrl = baseUrl;
    this.model = model;
    this.dimension = dimension;
    this.vectorizeServiceParameters = vectorizeServiceParameters;
    this.providerConfig = providerConfig;
  }

  /**
   * Applies a retry mechanism with backoff and jitter to the Uni returned by the embed() method,
   * which makes an HTTP request to a third-party service.
   *
   * @param <T> The type of the item emitted by the Uni.
   * @param uni The Uni to which the retry mechanism should be applied.
   * @return A Uni that will retry on the specified failures with the configured backoff and jitter.
   */
  protected <T> Uni<T> applyRetry(Uni<T> uni) {
    return uni.onFailure(
            throwable ->
                (throwable.getCause() != null
                        && throwable.getCause() instanceof JsonApiException jae
                        && jae.getErrorCode() == ErrorCodeV1.EMBEDDING_PROVIDER_TIMEOUT)
                    || throwable instanceof TimeoutException)
        .retry()
        .withBackOff(
            Duration.ofMillis(requestProperties.initialBackOffMillis()),
            Duration.ofMillis(requestProperties.maxBackOffMillis()))
        .withJitter(requestProperties.jitter())
        .atMost(requestProperties.atMostRetries());
  }

  /**
   * Checks if the vectorization will use an END_OF_LIFE model and throws an exception if it is.
   *
   * <p>As part of embedding model deprecation ability, any read and write with vectorization in an
   * END_OF_LIFE model will throw an exception.
   *
   * <p>Note, SUPPORTED and DEPRECATED models are still allowed to be used in read and write.
   *
   * <p>This method should be called before any vectorization operation.
   */
  protected void checkEOLModelUsage() {
    // Validate if the model is END_OF_LIFE
    if (model.apiModelSupport().status() == ApiModelSupport.SupportStatus.END_OF_LIFE) {
      throw SchemaException.Code.END_OF_LIFE_AI_MODEL.get(
          Map.of(
              "model",
              model.name(),
              "modelStatus",
              model.apiModelSupport().status().name(),
              "message",
              model
                  .apiModelSupport()
                  .message()
                  .orElse("The model is no longer supported (reached its end-of-life).")));
    }
  }

  /**
   * Vectorizes the given list of texts and returns the embeddings.
   *
   * @param texts List of texts to be vectorized
   * @param embeddingCredentials embeddingCredentials required for the provider
   * @param embeddingRequestType Type of request (INDEX or SEARCH)
   * @return VectorResponse
   */
  public abstract Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      EmbeddingCredentials embeddingCredentials,
      EmbeddingRequestType embeddingRequestType);

  /**
   * returns the maximum batch size supported by the provider
   *
   * @return
   */
  public abstract int maxBatchSize();

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
   * Helper method to replace parameters in a messageTemplate string with values from a map:
   * placeholders are of form {@code {parameterName}} and matching value to look for in the map is
   * String {@code "parameterName"}.
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

  /** Helper method to check if the API key is present in the header */
  protected void checkEmbeddingApiKeyHeader(String providerId, Optional<String> apiKey) {
    if (apiKey.isEmpty()) {
      throw EMBEDDING_PROVIDER_API_KEY_MISSING.toApiException(
          "header value `%s` is missing for embedding provider: %s",
          EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME, providerId);
    }
  }

  /**
   * Record to hold the batchId and embedding vectors
   *
   * @param batchId - Sequence number for the batch to order the vectors.
   * @param embeddings - Embedding vectors for the given text inputs.
   */
  public record Response(int batchId, List<float[]> embeddings) implements Recordable {

    public static Response of(int batchId, List<float[]> embeddings) {
      return new Response(batchId, embeddings);
    }

    @Override
    public DataRecorder recordTo(DataRecorder dataRecorder) {
      return dataRecorder.append("batchId", batchId).append("embeddings", embeddings);
    }
  }

  public enum EmbeddingRequestType {
    /** This is used when vectorizing data in write operation for indexing */
    INDEX,
    /** This is used when vectorizing data for search operation */
    SEARCH
  }
}
