package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.List;
import java.util.Objects;

/**
 * What a findAndRerank command will do, decided by the {@link FindAndRerankPlanner}.
 *
 * @param legs The reads that find the candidate documents, never empty. Their order is the order
 *     the read tasks are built in.
 * @param rerankServiceDef The reranking service to use, either the override from the command or the
 *     collection default.
 * @param rerankingQuery The query to rerank the documents with.
 * @param passageLocator Locates the passage to rerank on in each document.
 * @param limit The maximum number of documents to return after reranking.
 */
public record FindAndRerankPlan(
    List<RerankLeg> legs,
    CollectionRerankDef.RerankServiceDef rerankServiceDef,
    RerankingQuery rerankingQuery,
    PathMatchLocator passageLocator,
    int limit)
    implements Recordable {

  public FindAndRerankPlan {
    Objects.requireNonNull(legs, "legs must not be null");
    // safety, the planner throws a request error before this, and the rerank task cannot work
    // without any reads
    if (legs.isEmpty()) {
      throw new IllegalArgumentException("legs must not be empty");
    }
    legs = List.copyOf(legs);
    Objects.requireNonNull(rerankServiceDef, "rerankServiceDef must not be null");
    Objects.requireNonNull(rerankingQuery, "rerankingQuery must not be null");
    Objects.requireNonNull(passageLocator, "passageLocator must not be null");
  }

  @Override
  public String toString() {
    return PrettyPrintable.print(this);
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    // only the provider and model of the service, not its authentication or parameters
    return dataRecorder
        .append("legs", Recordable.copyOf(legs))
        .append("rerankServiceDef.provider", rerankServiceDef.provider())
        .append("rerankServiceDef.modelName", rerankServiceDef.modelName())
        .append("rerankingQuery", rerankingQuery)
        .append("passageLocator.path", passageLocator.path())
        .append("limit", limit);
  }
}
