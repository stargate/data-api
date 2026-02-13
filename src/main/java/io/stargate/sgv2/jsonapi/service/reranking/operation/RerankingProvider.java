package io.stargate.sgv2.jsonapi.service.reranking.operation;

import static jakarta.ws.rs.core.Response.Status.Family.CLIENT_ERROR;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.RerankingProviderException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.provider.*;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A provider for Embedding models , using {@link ModelType#RERANKING} */
public abstract class RerankingProvider extends ProviderBase {

  protected static final Logger LOGGER = LoggerFactory.getLogger(RerankingProvider.class);

  protected final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig;

  protected final Duration initialBackOffDuration;

  protected final Duration maxBackOffDuration;

  protected RerankingProvider(
      ModelProvider modelProvider,
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig modelConfig) {
    super(
        modelProvider,
        ModelType.RERANKING,
        new RerankingProviderExceptionHandler(modelProvider, ModelType.RERANKING));

    this.modelConfig = modelConfig;

    this.initialBackOffDuration =
        Duration.ofMillis(modelConfig.properties().initialBackOffMillis());
    this.maxBackOffDuration = Duration.ofMillis(modelConfig.properties().maxBackOffMillis());
  }

  @Override
  public String modelName() {
    return modelConfig.name();
  }

  @Override
  public ApiModelSupport modelSupport() {
    return modelConfig.apiModelSupport();
  }

  public ModelUsage createEmptyModelUsage(RerankingCredentials rerankingCredentials) {
    return createModelUsage(
        rerankingCredentials.tenant(), ModelInputType.INPUT_TYPE_UNSPECIFIED, 0, 0, 0, 0, 0);
  }

  /**
   * Reranks the texts, batching as needed, and returns a final RerankingResponse as the original
   * order of the passages with the reranking score.
   *
   * <p>E.G. if the original passages list is <code>["a", "b", "c", "d", "e"]</code> and the micro
   * batch is 2, then API will do 3 batch reranking calls: <code>["a", "b"], ["c", "d"], ["e"]
   * </code> 3 response will be returned:
   *
   * <ul>
   *   <li>batch 0: <code>[{index:1, score:x1}, {index:0, score:x2}]</code>
   *   <li>batch 1: <code>[{index:0, score:x3}, {index:1, score:x4}]</code>
   *   <li>batch 2: <code>[{index:0, score:x5}]</code>
   * </ul>
   *
   * Then this method will adjust the indices and return the final response: <code>
   * [{index:0, score:x1},
   * {index:1, score:x2}, {index:2, score:x3}, {index:3, score:x4}, {index:4, score:x5}]</code>
   */
  public Uni<RerankingResponse> rerank(
      String query, List<String> passages, RerankingCredentials rerankingCredentials) {

    // TODO: what to do if passages is empty?
    List<List<String>> passageBatches = createPassageBatches(passages);
    List<Uni<BatchedRerankingResponse>> batchRerankings = new ArrayList<>();

    for (int batchId = 0; batchId < passageBatches.size(); batchId++) {
      batchRerankings.add(
          rerank(batchId, query, passageBatches.get(batchId), rerankingCredentials));
    }

    return Uni.join().all(batchRerankings).andFailFast().map(this::aggregateRanks);
  }

  /**
   * Subclasses must implement to do the reranking, after the batching is done.
   *
   * <p>... <b>NOTE:</b> This is public because the embedding Gateway currently needs to call it,
   * use the {@link #rerank(String, List, RerankingCredentials)} method instead.
   */
  public abstract Uni<BatchedRerankingResponse> rerank(
      int batchId, String query, List<String> passages, RerankingCredentials rerankingCredentials);

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
    return modelConfig.properties().jitter();
  }

  @Override
  protected int atMostRetries() {
    return modelConfig.properties().atMostRetries();
  }

  @Override
  protected boolean decideRetry(Throwable throwable) {
    boolean retry =
        throwable instanceof RerankingProviderException rpe
            && rpe.code.equals(RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.name());
    return retry || super.decideRetry(throwable);
  }

  @Override
  protected RuntimeException mapHTTPError(Response jakartaResponse, String errorMessage) {

    if (jakartaResponse.getStatus() == Response.Status.REQUEST_TIMEOUT.getStatusCode()
        || jakartaResponse.getStatus() == Response.Status.GATEWAY_TIMEOUT.getStatusCode()) {
      return RerankingProviderException.Code.RERANKING_PROVIDER_TIMEOUT.get(
          Map.of(
              "modelProvider", modelProvider().apiName(),
              "httpStatus", String.valueOf(jakartaResponse.getStatus()),
              "errorMessage", errorMessage));
    }

    if (jakartaResponse.getStatus() == Response.Status.TOO_MANY_REQUESTS.getStatusCode()) {
      return SchemaException.Code.RERANKING_PROVIDER_RATE_LIMITED.get(
          Map.of(
              "errorMessage",
              "Provider: %s; HTTP Status: %s; Error Message: %s"
                  .formatted(
                      modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage)));
    }

    if (jakartaResponse.getStatusInfo().getFamily() == CLIENT_ERROR) {
      return SchemaException.Code.RERANKING_PROVIDER_CLIENT_ERROR.get(
          Map.of(
              "errorMessage",
              "Provider: %s; HTTP Status: %s; Error Message: %s"
                  .formatted(
                      modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage)));
    }

    if (jakartaResponse.getStatusInfo().getFamily() == Response.Status.Family.SERVER_ERROR) {
      return SchemaException.Code.RERANKING_PROVIDER_SERVER_ERROR.get(
          Map.of(
              "errorMessage",
              "Provider: %s; HTTP Status: %s; Error Message: %s"
                  .formatted(
                      modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage)));
    }

    // All other errors, Should never happen as all errors are covered above
    return SchemaException.Code.RERANKING_PROVIDER_UNEXPECTED_RESPONSE.get(
        Map.of(
            "errorMessage",
            "Provider: %s; HTTP Status: %s; Error Message: %s"
                .formatted(modelProvider().apiName(), jakartaResponse.getStatus(), errorMessage)));
  }

  /** Create batches of passages to be reranked. */
  private List<List<String>> createPassageBatches(List<String> passages) {

    List<List<String>> batches = new ArrayList<>();
    for (int i = 0; i < passages.size(); i += modelConfig.properties().maxBatchSize()) {
      batches.add(
          passages.subList(
              i, Math.min(i + modelConfig.properties().maxBatchSize(), passages.size())));
    }
    return batches;
  }

  /** Aggregate the ranks from all batched reranking calls. */
  private RerankingResponse aggregateRanks(List<BatchedRerankingResponse> batchResponses) {

    List<Rank> finalRanks = new ArrayList<>();
    ModelUsage aggregatedModelUsage = null;

    for (BatchedRerankingResponse batchResponse : batchResponses) {
      int batchStartIndex = batchResponse.batchId() * modelConfig.properties().maxBatchSize();

      aggregatedModelUsage =
          aggregatedModelUsage == null
              ? batchResponse.modelUsage()
              : aggregatedModelUsage.merge(batchResponse.modelUsage());
      for (Rank rank : batchResponse.ranks()) {
        finalRanks.add(new Rank(batchStartIndex + rank.index(), rank.score()));
      }
    }
    // This is the original order of all the passages.
    finalRanks.sort(Comparator.comparingInt(Rank::index));
    return new RerankingResponse(finalRanks, aggregatedModelUsage);
  }

  /**
   * Unbatched reranking response, returned from the public {@link #rerank(String, List,
   * RerankingCredentials)}
   *
   * <p>...
   */
  public record RerankingResponse(List<Rank> ranks, ModelUsage modelUsage) {}

  /**
   * Unbatched reranking response, returned from the protected {@link #rerank(int, String, List,
   * RerankingCredentials)}
   *
   * <p>...
   */
  public record BatchedRerankingResponse(int batchId, List<Rank> ranks, ModelUsage modelUsage) {}

  /**
   * Individual rank and the index of the input passage.
   *
   * <p>...
   */
  public record Rank(int index, float score) {}
}
