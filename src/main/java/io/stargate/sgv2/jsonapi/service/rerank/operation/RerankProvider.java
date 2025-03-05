package io.stargate.sgv2.jsonapi.service.rerank.operation;

import io.smallrye.mutiny.Uni;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RerankProvider {
  protected static final Logger logger = LoggerFactory.getLogger(RerankProvider.class);
  protected final String baseUrl;
  protected final String modelName;

  protected RerankProvider() {
    this(null, null);
  }

  protected RerankProvider(String baseUrl, String modelName) {
    this.baseUrl = baseUrl;
    this.modelName = modelName;
  }

  public abstract Uni<Response> rerank(int batchId, String query, List<String> passages);

  public record Response(int batchId, List<Rank> ranks) {
    public static Response of(int batchId, List<Rank> ranks) {
      return new Response(batchId, ranks);
    }
  }

  public record Rank(int index, float logit) {}
}
