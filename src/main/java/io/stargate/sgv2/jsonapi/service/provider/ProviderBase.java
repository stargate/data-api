package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.embedding.gateway.EmbeddingGateway;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base for model providers such as embedding and reranking. */
public abstract class ProviderBase {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ProviderBase.class);

  private final ModelProvider modelProvider;
  private final ModelType modelType;
  private final String modelName;

  protected ProviderBase(ModelProvider modelProvider, ModelType modelType, String modelName) {
    this.modelProvider = modelProvider;
    this.modelType = modelType;
    this.modelName = modelName;
  }

  public String modelName() {
    return modelName;
  }

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

    return uni.onItem()
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

    if (jakartaResponse.getStatus() >= 400) {
      var runtimeException = handleHTTPError(jakartaResponse);
      if (runtimeException != null) {
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

  protected RuntimeException handleHTTPError(Response jakartaResponse) {

    var errorMessage = responseErrorMessage(jakartaResponse);
    LOGGER.error(
        "Error response from model provider, modelProvider: {}, modelName:{}, http.status: {}, error: {}",
        modelProvider,
        modelName,
        jakartaResponse.getStatus(),
        errorMessage);

    return mapHTTPError(jakartaResponse, errorMessage);
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
          modelName,
          e);
    }

    return (rootNode == null) ? raw : responseErrorMessage(rootNode);
  }

  protected String responseErrorMessage(JsonNode rootNode) {

    var messageNode = rootNode.at(errorMessageJsonPtr());
    return messageNode.isMissingNode() ? rootNode.toString() : messageNode.toString();
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
        modelName,
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
