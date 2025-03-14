package io.stargate.sgv2.jsonapi.service.reranking.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.reranking.configuration.RerankingProvidersConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RerankingProvider {
  protected static final Logger logger = LoggerFactory.getLogger(RerankingProvider.class);
  protected final String baseUrl;
  protected final String modelName;
  protected final RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties
      requestProperties;

  protected RerankingProvider(
      String baseUrl,
      String modelName,
      RerankingProvidersConfig.RerankingProviderConfig.ModelConfig.RequestProperties
          requestProperties) {
    this.baseUrl = baseUrl;
    this.modelName = modelName;
    this.requestProperties = requestProperties;
  }

  /**
   * Gather the results from all batch rerank calls, adjust the indices, so they refer to the
   * original passages list, and return a final RerankResponse as the original order of the passages
   * with the rerank score.
   *
   * <p>E.G. if the original passages list is ["a", "b", "c", "d", "e"] and the micro batch is 2,
   * then API will do 3 batch rerank calls: ["a", "b"], ["c", "d"], ["e"]. 3 response will be
   * returned:
   *
   * <ul>
   *   <li>batch 0: [{index:1, score:x1}, {index:0, score:x2}]
   *   <li>batch 1: [{index:0, score:x3}, {index:1, score:x4}]
   *   <li>batch 2: [{index:0, score:x5}]
   * </ul>
   *
   * Then this method will adjust the indices and return the final response: [{index:0, score:x1},
   * {index:1, score:x2}, {index:2, score:x3}, {index:3, score:x4}, {index:4, score:x5}]
   */
  public Uni<RerankResponse> rerank(
      String query, List<String> passages, RerankingCredentials rerankingCredentials) {
    int maxBatch = requestProperties.maxBatchSize();
    List<Uni<RerankBatchResponse>> batchReranks = new ArrayList<>();
    for (int i = 0; i < passages.size(); i += maxBatch) {
      int batchId = i / maxBatch;
      List<String> batch = passages.subList(i, Math.min(i + maxBatch, passages.size()));
      batchReranks.add(rerank(batchId, query, batch, rerankingCredentials));
    }
    return Uni.join()
        .all(batchReranks)
        .andFailFast()
        .map(
            batchResponses -> {
              List<Rank> finalRanks = new ArrayList<>();
              for (RerankBatchResponse batchResponse : batchResponses) {
                int batchStartIndex = batchResponse.batchId() * maxBatch;
                for (Rank rank : batchResponse.ranks()) {
                  finalRanks.add(new Rank(batchStartIndex + rank.index(), rank.score()));
                }
              }
              // This is the original order of the passages
              finalRanks.sort(Comparator.comparingInt(Rank::index));
              return RerankResponse.of(finalRanks);
            });
  }

  public record RerankResponse(List<Rank> ranks) {
    public static RerankResponse of(List<Rank> ranks) {
      return new RerankResponse(ranks);
    }
  }

  /** Micro batch rerank method, which will rerank a batch of passages. */
  public abstract Uni<RerankBatchResponse> rerank(
      int batchId, String query, List<String> passages, RerankingCredentials rerankingCredentials);

  /** The response of a batch rerank call. */
  public record RerankBatchResponse(int batchId, List<Rank> ranks, Usage usage) {
    public static RerankBatchResponse of(int batchId, List<Rank> rankings, Usage usage) {
      return new RerankBatchResponse(batchId, rankings, usage);
    }
  }

  public record Rank(int index, float score) {}

  public record Usage(int prompt_tokens, int total_tokens) {}

  /**
   * Applies a retry mechanism with backoff and jitter to the Uni returned by the rerank() method,
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
}
