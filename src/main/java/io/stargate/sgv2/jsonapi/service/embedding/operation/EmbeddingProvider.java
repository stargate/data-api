package io.stargate.sgv2.jsonapi.service.embedding.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingProviderConfigStore;
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
  protected final String modelName;
  protected final int dimension;
  protected final Map<String, Object> vectorizeServiceParameters;

  /** Default constructor */
  protected EmbeddingProvider() {
    this(null, null, null, 0, null);
  }

  /** Constructs an EmbeddingProvider with the specified configuration. */
  protected EmbeddingProvider(
      EmbeddingProviderConfigStore.RequestProperties requestProperties,
      String baseUrl,
      String modelName,
      int dimension,
      Map<String, Object> vectorizeServiceParameters) {
    this.requestProperties = requestProperties;
    this.baseUrl = baseUrl;
    this.modelName = modelName;
    this.dimension = dimension;
    this.vectorizeServiceParameters = vectorizeServiceParameters;
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
                        && jae.getErrorCode() == ErrorCode.EMBEDDING_PROVIDER_TIMEOUT)
                    || throwable instanceof TimeoutException)
        .retry()
        .withBackOff(
            Duration.ofMillis(requestProperties.initialBackOffMillis()),
            Duration.ofMillis(requestProperties.maxBackOffMillis()))
        .withJitter(requestProperties.jitter())
        .atMost(requestProperties.atMostRetries());
  }

  /**
   * Vectorizes the given list of texts and returns the embeddings.
   *
   * @param texts List of texts to be vectorized
   * @param credentials Credentials required for the provider
   * @param embeddingRequestType Type of request (INDEX or SEARCH)
   * @return VectorResponse
   */
  public abstract Uni<Response> vectorize(
      int batchId,
      List<String> texts,
      Credentials credentials,
      EmbeddingRequestType embeddingRequestType);

  /**
   * Record to hold the credentials required for the provider
   *
   * @param apiKey Optional API key for the all providers other than AWS Bedrock. If not provided,
   *     the default API key will be used.
   * @param accessKeyId AWS access key id
   * @param secretAccessKey AWS secret access key
   */
  public record Credentials(
      Optional<String> apiKey, Optional<String> accessKeyId, Optional<String> secretAccessKey) {}

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
   * Helper method to replace parameters in a template string with values from a map: placeholders
   * are of form {@code {parameterName}} and matching value to look for in the map is String {@code
   * "parameterName"}.
   *
   * @param template Template with placeholders to replace
   * @param parameters Parameters to replace in the template
   * @return Processed template with replaced parameters
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
        throw new IllegalArgumentException(
            "Missing URL parameter '" + key + "' (available: " + parameters.keySet() + ")");
      }
      baseUrl.append(value);
    }
    return baseUrl.toString();
  }

  /**
   * Record to hold the batchId and embedding vectors
   *
   * @param batchId - Sequence number for the batch to order the vectors.
   * @param embeddings - Embedding vectors for the given text inputs.
   */
  public record Response(int batchId, List<float[]> embeddings) {
    public static Response of(int batchId, List<float[]> embeddings) {
      return new Response(batchId, embeddings);
    }
  }

  public enum EmbeddingRequestType {
    /** This is used when vectorizing data in write operation for indexing */
    INDEX,
    /** This is used when vectorizing data for search operation */
    SEARCH
  }
}
