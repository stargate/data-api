package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base for model providers such as embedding and reranking. */
public abstract class ProviderBase {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ProviderBase.class);

  private final ModelProvider modelProvider;
  private final ModelType modelType;

  // TODO: the Embedding and Rerank code *does not* share model configs, but they can & should do,
  // so we we cannot pass the model into the base until we refactor the code.
  protected ProviderBase(ModelProvider modelProvider, ModelType modelType) {
    this.modelProvider = modelProvider;
    this.modelType = modelType;
  }

  public abstract String modelName();

  public abstract ApiModelSupport modelSupport();

  public ModelProvider modelProvider() {
    return modelProvider;
  }

  protected abstract String errorMessageJsonPtr();

  protected abstract Duration initialBackOffDuration();

  protected abstract Duration maxBackOffDuration();

  protected abstract double jitter();

  protected abstract int atMostRetries();

  /**
   * Applies a retry mechanism with backoff and jitter to the Uni returned by the rerank() method,
   * which makes an HTTP request to a third-party service.
   *
   * @param <T> The type of the item emitted by the Uni.
   * @param uni The Uni to which the retry mechanism should be applied.
   * @return A Uni that will retry on the specified failures with the configured backoff and jitter.
   */
  protected Uni<Response> retryHTTPCall(Uni<Response> uni) {

    return uni
        // Catch *any* failure early, convert it to a Response if possible
        .onFailure(WebApplicationException.class)
        .recoverWithItem(ex -> ((WebApplicationException) ex).getResponse())
        .onItem()
        .transform(this::handleHTTPResponse)
        .onFailure(this::decideRetry)
        .retry()
        .withBackOff(initialBackOffDuration(), maxBackOffDuration())
        .withJitter(jitter())
        .atMost(atMostRetries());
  }

  protected boolean decideRetry(Throwable throwable) {
    return throwable instanceof TimeoutException;
  }

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

  protected RuntimeException mapHTTPError(Response jakartaResponse) {

    var errorMessage = responseErrorMessage(jakartaResponse);
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

  protected abstract RuntimeException mapHTTPError(Response response, String errorMessage);

  protected String responseErrorMessage(Response jakartaResponse) {

    MediaType contentType = jakartaResponse.getMediaType();
    String raw = jakartaResponse.readEntity(String.class);

    if (contentType == null || !MediaType.APPLICATION_JSON_TYPE.isCompatible(contentType)) {
      LOGGER.error(
          "Non-JSON error response from model provider, modelProvider:{}, modelName: {}, raw:{}",
          modelProvider(),
          modelName(),
          raw);
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

  protected String responseErrorMessage(JsonNode rootNode) {

    var messageNode = rootNode.at(errorMessageJsonPtr());
    return messageNode.isMissingNode() ? rootNode.toString() : messageNode.toString();
  }

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
      String tenantId,
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
        tenantId,
        modelInputType,
        promptTokens,
        totalTokens,
        requestBytes,
        responseBytes,
        durationNanos);
  }

  protected ModelUsage createModelUsage(
      String tenantId,
      ModelInputType modelInputType,
      int promptTokens,
      int totalTokens,
      Response jakartaResponse,
      long durationNanos) {

    return createModelUsage(
        tenantId,
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
