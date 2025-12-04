package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for model providers of any model type, such as embedding and reranking.
 *
 * <p>Notes for implementors:
 *
 * <ul>
 *   <li>Define the rest easy client to return a {@link Response} and call {@link
 *       #retryHTTPCall(Uni)} to manage retries and backoff.
 *   <li>Once you have the response called {@link #decodeResponse(Response, Class)} to decode the
 *       response to a specific class.
 * </ul>
 *
 * <p>. The Embedding and Rerank code *does not* share model configs, but they can & should do, so
 * we cannot pass the model into the base until we refactor the code. That is why there are
 * properties that could be removed or refactored if we had more common config.
 */
public abstract class ProviderBase {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ProviderBase.class);

  // There is not a MediaType for text/json in Jakarta
  private static final MediaType MEDIATYPE_TEXT_JSON = new MediaType("text", "json");

  protected static final Predicate<MediaType> IS_JSON_MEDIA_TYPE =
      mediaType ->
          MediaType.APPLICATION_JSON_TYPE.isCompatible(mediaType)
              || MEDIATYPE_TEXT_JSON.isCompatible(mediaType);

  private final ModelProvider modelProvider;
  private final ModelType modelType;

  protected ProviderBase(ModelProvider modelProvider, ModelType modelType) {
    this.modelProvider = modelProvider;
    this.modelType = modelType;
  }

  public ModelProvider modelProvider() {
    return modelProvider;
  }

  public abstract String modelName();

  public abstract ApiModelSupport modelSupport();

  /**
   * Called to map the HTTP response to an API exception, subclasses should override this method to
   * provide a specific mapping for the model provider.
   *
   * <p>This method is called after the error message is extracted from the response.
   *
   * @param response The raw HTTP response from the model provider
   * @param errorMessage The error message extracted from the response
   * @return The mapped exception to later throw, should not return null.
   */
  protected abstract RuntimeException mapHTTPError(Response response, String errorMessage);

  /**
   * Last in the chain to extract the error message from the response JSON, see {@link
   * #responseErrorMessage(Response)}
   *
   * <p>
   *
   * @return JSON Pointer path that will be used with {@link JsonNode#at(String)} to extract the
   *     error message node.
   */
  protected abstract String errorMessageJsonPtr();

  protected abstract Duration initialBackOffDuration();

  protected abstract Duration maxBackOffDuration();

  protected abstract double jitter();

  protected abstract int atMostRetries();

  /**
   * Retries the HTTP call with backoff and jitter, and translates the response to a API exception.
   */
  protected Uni<Response> retryHTTPCall(Uni<Response> uni) {

    return uni
        // Catch *any* web exception from jakarta rest client
        .onFailure(WebApplicationException.class)
        // and recover with the jakarta response, so we can translate to API exception
        .recoverWithItem(ex -> ((WebApplicationException) ex).getResponse())
        .onItem()
        // handle the response, throws if there is an error
        .transform(this::handleHTTPResponse)
        // decide if we want to retry
        .onFailure(this::decideRetry)
        .retry()
        .withBackOff(initialBackOffDuration(), maxBackOffDuration())
        .withJitter(jitter())
        .atMost(atMostRetries());
  }

  /**
   * Called to determine if the operation should be retried based on the throwable.
   *
   * <p>Subclasses should normally override, and then call the base if they do not want to retry.
   *
   * @param throwable Exception, either the API Exception mapped from the jakarta response. Or any
   *     other error if the rest client throws non WebApplicationException
   * @return <code>true</code> if the operation should be retried, <code>false</code> otherwise.
   */
  protected boolean decideRetry(Throwable throwable) {
    return throwable instanceof TimeoutException;
  }

  /**
   * Called to process the HTTP response from the model provider, called for both successful and
   * error responses. This function determines if the response is an error.
   *
   * <p>Implementatioms shoudl <code>throw</code> any exceptions created from the response
   *
   * @param jakartaResponse Raw HTTP response from the model provider, which may be an error
   *     response.
   * @return The original response if it is successful, or throws an exception if it is an error.
   */
  protected Response handleHTTPResponse(Response jakartaResponse) {

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "handleHTTPResponse() - got response, modelProvider: {}, modelName: {}, response.status: {}, response.headers: {}",
          modelProvider(),
          modelName(),
          jakartaResponse.getStatus(),
          jakartaResponse.getHeaders());
    }

    if (jakartaResponse.getStatus() >= 400) {
      var runtimeException = mapHTTPError(jakartaResponse);
      if (runtimeException != null) {
        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(
              "handleHTTPResponse() - http response mapped to error, runtimeException: {}",
              runtimeException.toString());
        }
        throw runtimeException;
      }

      throw new IllegalStateException(
          String.format(
              "Unhandled error from model provider, modelProvider: %s, modelName: %s, status: %d, responseBody: %s",
              modelProvider(),
              modelName(),
              jakartaResponse.getStatus(),
              jakartaResponse.readEntity(String.class)));
    }
    return jakartaResponse;
  }

  /**
   * Called to map the HTTP response to an API exception, sublcasses should override {@link
   * #mapHTTPError(Response, String)} which is called after the error message is extracted from the
   * response.
   *
   * <p>Should only be called when there response status is >= 400, i.e. an error response.
   *
   * @param jakartaResponse The raw HTTP response from the model provider
   * @return The mapped exception to later throw, should not return null.
   */
  protected RuntimeException mapHTTPError(Response jakartaResponse) {

    var errorMessage = responseErrorMessage(jakartaResponse);
    // this is the main "error" log when the response is an error
    LOGGER.error(
        "Error response from model provider, modelProvider: {}, modelName:{}, http.status: {}, error: {}",
        modelProvider,
        modelName(),
        jakartaResponse.getStatus(),
        errorMessage);

    var mappedException = mapHTTPError(jakartaResponse, errorMessage);
    if (mappedException != null) {
      return mappedException;
    }

    return new IllegalStateException(
        String.format(
            "Unhandled error from model provider, modelProvider: %s, modelName: %s, status: %d, responseBody: %s",
            modelProvider(),
            modelName(),
            jakartaResponse.getStatus(),
            jakartaResponse.readEntity(String.class)));
  }

  /**
   * First in the chain to extract the error message from the response, the easiest thing for
   * subclasses is to override {@link #errorMessageJsonPtr()} to provide the JSON Pointer to get a
   * single error message from the response JSON.
   *
   * <p>This method decodes the JSON response and calles {@link #responseErrorMessage(JsonNode)}
   */
  protected String responseErrorMessage(Response jakartaResponse) {

    MediaType contentType = jakartaResponse.getMediaType();
    String raw = jakartaResponse.readEntity(String.class);

    if (contentType == null || !IS_JSON_MEDIA_TYPE.test(contentType)) {
      // we have an error, only need a debug
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Non-JSON error response from model provider, modelProvider:{}, modelName: {}, raw:{}",
            modelProvider(),
            modelName(),
            raw);
      }
      return raw;
    }

    JsonNode rootNode = null;
    try {
      rootNode = jakartaResponse.readEntity(JsonNode.class);
    } catch (Exception e) {
      // If we cannot read the response as JsonNode, log the error and return the raw response
      LOGGER.error(
          "Error parsing error JSON from reranking provider, modelProvider: {},  modelName: {}",
          modelProvider,
          modelName(),
          e);
    }

    return (rootNode == null) ? raw : responseErrorMessage(rootNode);
  }

  /**
   * Secong in the chain to extract the error message from the response JSON, this is called with
   * the decoded JSON node. The easiest thing for subclasses is to override {@link
   * #errorMessageJsonPtr()}
   */
  protected String responseErrorMessage(JsonNode rootNode) {

    var messageNode = rootNode.at(errorMessageJsonPtr());
    return messageNode.isMissingNode() ? rootNode.toString() : messageNode.toString();
  }

  /**
   * Utility method to decode the response (JSON entity) to a specific class, and log if there is an
   * error.
   *
   * <p>Because decoding happens after all the retry, it will not be mapped into an API exception
   * through same proccess as making the HTTP calls.
   */
  protected <T> T decodeResponse(Response jakartaResponse, Class<T> responseClass) {
    try {
      return jakartaResponse.readEntity(responseClass);
    } catch (Throwable e) {
      LOGGER.error(
          "decodeResponse() - error decoding response modelProvider: {}, modelName: {}, responseClass: {}",
          modelProvider(),
          modelName(),
          responseClass.getName(),
          e);
      // rethrow so it can be handled elsewhere, we just want to log the error
      throw e;
    }
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

    if (modelSupport().status() == ApiModelSupport.SupportStatus.END_OF_LIFE) {
      throw SchemaException.Code.END_OF_LIFE_AI_MODEL.get(
          Map.of(
              "model",
              modelName(),
              "modelStatus",
              modelSupport().status().name(),
              "message",
              modelSupport()
                  .message()
                  .orElse("The model is no longer supported (reached its end-of-life).")));
    }
  }

  protected ModelUsage createModelUsage(
      Tenant tenant,
      ModelInputType modelInputType,
      int promptTokens,
      int totalTokens,
      int requestBytes,
      int responseBytes,
      long durationNanos) {

    return new ModelUsage(
        modelProvider,
        modelType,
        modelName(),
        tenant,
        modelInputType,
        promptTokens,
        totalTokens,
        requestBytes,
        responseBytes,
        durationNanos);
  }

  protected ModelUsage createModelUsage(
      Tenant tenant,
      ModelInputType modelInputType,
      int promptTokens,
      int totalTokens,
      Response jakartaResponse,
      long durationNanos) {

    return createModelUsage(
        tenant,
        modelInputType,
        promptTokens,
        totalTokens,
        ProviderHttpInterceptor.getSentBytes(jakartaResponse),
        ProviderHttpInterceptor.getReceivedBytes(jakartaResponse),
        durationNanos);
  }

  protected ModelUsage createModelUsage(EmbeddingGateway.ModelUsage gatewayModelUsage) {
    return ModelUsage.fromEmbeddingGateway(gatewayModelUsage);
  }
}
