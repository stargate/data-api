package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.ResponseData;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.TraceMessage;
import io.stargate.sgv2.jsonapi.api.request.RerankingCredentials;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProvider;
import io.stargate.sgv2.jsonapi.util.PathMatchLocator;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RerankingTask<SchemaT extends TableBasedSchemaObject>
    extends BaseTask<
        SchemaT, RerankingTask.RerankingResultSupplier, RerankingTask.RerankingTaskResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RerankingTask.class);

  private final RerankingProvider rerankingProvider;
  private final String query;
  private final PathMatchLocator passageLocator;
  private final DocumentProjector userProjection;
  private final List<DeferredCommandWithSource> deferredReads;
  private final int limit;

  private float[] sortVector = null;
  // captured in onSuccess
  private RerankingTaskResult rerankingTaskResult;

  public RerankingTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      RerankingProvider rerankingProvider,
      String query,
      PathMatchLocator passageLocator,
      DocumentProjector userProjection,
      List<DeferredCommandWithSource> deferredReads,
      int limit) {
    super(position, schemaObject, retryPolicy);

    this.rerankingProvider = rerankingProvider;
    this.query = query;
    this.passageLocator = Objects.requireNonNull(passageLocator, "passageLocator must not be null");
    this.userProjection = userProjection;
    this.deferredReads = deferredReads;
    this.limit = limit;

    setStatus(TaskStatus.READY);
  }

  public static <SchemaT extends TableBasedSchemaObject> RerankingTaskBuilder<SchemaT> builder(
      CommandContext<SchemaT> commandContext) {
    return new RerankingTaskBuilder<>(commandContext);
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  @Override
  protected RerankingResultSupplier buildResultSupplier(CommandContext<SchemaT> commandContext) {

    // TODO: add a flag to know if we need to do this ?

    // Find the sort vector that was used for the inner query, if any
    // For now there should only ever be one sort vector included
    for (var deferredAndSource : deferredReads) {

      // If we are being called to run, the deferred reads we were waiting for should be completed.
      // This will throw if that is not the case.
      var rawReadResult = deferredAndSource.deferredRead.commandResult();

      float[] candidateSortVector = (float[]) rawReadResult.status().get(CommandStatus.SORT_VECTOR);
      if (candidateSortVector != null) {
        if (sortVector == null) {
          sortVector = candidateSortVector;
        } else if (!Arrays.equals(sortVector, candidateSortVector)) {
          throw new IllegalStateException("Multiple sort vectors found in raw read results");
        } else {
          sortVector = candidateSortVector;
        }
      }
    }

    var dedupResult = deduplicateResults();

    commandContext
        .requestTracing()
        .maybeTrace(
            "De-duplicated for reranking, reads=%s, total documents=%s, dropped documents=%s, deduplicated documents=%s"
                .formatted(
                    dedupResult.totalReads(),
                    dedupResult.totalDocuments(),
                    dedupResult.droppedDocuments(),
                    dedupResult.deduplicatedDocuments.size()));

    return new RerankingResultSupplier(
        commandContext.requestTracing(),
        rerankingProvider,
        commandContext.requestContext().getRerankingCredentials(),
        query,
        passageLocator,
        dedupResult.deduplicatedDocuments(),
        limit);
  }

  @Override
  protected RuntimeException maybeHandleException(
      RerankingResultSupplier resultSupplier, RuntimeException runtimeException) {
    return runtimeException;
  }

  @Override
  protected void onSuccess(RerankingTaskResult result) {
    this.rerankingTaskResult = result;
    super.onSuccess(result);
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder)
        .append("rerankingProvider", rerankingProvider)
        .append("query", query)
        .append("passageLocator", passageLocator)
        .append("limit", limit);
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  RerankingTaskResult rerankingTaskResult() {
    checkStatus("rerankingTaskResult()", TaskStatus.COMPLETED);
    return rerankingTaskResult;
  }

  float[] sortVector() {
    return sortVector;
  }

  public record DeferredCommandWithSource(
      Rank.RankSource rankSource, DeferredCommandResult deferredRead) {}

  private record DeduplicationResult(
      int totalReads,
      int totalDocuments,
      int droppedDocuments,
      List<ScoredDocument> deduplicatedDocuments) {}

  private DeduplicationResult deduplicateResults() {

    if (deferredReads.isEmpty()) {
      throw new IllegalArgumentException("deferredReads must not be empty");
    }

    // This code relies on working with collections where the documents all have _id
    // will need to change for tables and rows
    ScoredDocumentMerger merger = null;
    int totalReads = 0;

    for (var deferredAndSource : deferredReads) {
      var commandResult = deferredAndSource.deferredRead().commandResult();
      if (commandResult == null) {
        throw new IllegalStateException(
            "Deferred read from source %s returned null commandResult"
                .formatted(deferredAndSource.rankSource()));
      }
      var multiDocResponse = (ResponseData.MultiResponseData) commandResult.data();
      totalReads++;
      if (merger == null) {
        merger =
            new ScoredDocumentMerger(
                multiDocResponse.documents().size() * 2, passageLocator, userProjection);
      }
      // rank must start at 1
      int rank = 1;
      for (var doc : multiDocResponse.documents()) {
        merger.merge(rank++, deferredAndSource.rankSource(), doc);
      }
    }

    var deduplicatedDocuments = merger.mergedDocuments();
    LOGGER.debug(
        "deduplicateResults() - seenDocuments={}, droppedDocuments={}, deduplicatedDocuments.size()={}",
        merger.seenDocuments(),
        merger.droppedDocuments(),
        deduplicatedDocuments.size());
    return new DeduplicationResult(
        totalReads, merger.seenDocuments(), merger.droppedDocuments(), deduplicatedDocuments);
  }

  /**
   * Calls the reranking provider to get the ranking
   *
   * <p>Needs to be static for the generics to work
   */
  public static class RerankingResultSupplier implements UniSupplier<RerankingTaskResult> {

    private final RequestTracing requestTracing;
    private final RerankingProvider rerankingProvider;
    private final RerankingCredentials credentials;
    private final String query;
    private final List<ScoredDocument> unrankedDocs;
    private final int limit;

    RerankingResultSupplier(
        RequestTracing requestTracing,
        RerankingProvider rerankingProvider,
        RerankingCredentials credentials,
        String query,
        PathMatchLocator passageLocator,
        List<ScoredDocument> unrankedDocs,
        int limit) {
      this.requestTracing = requestTracing;
      this.rerankingProvider = rerankingProvider;
      this.credentials = credentials;
      this.query = query;
      this.unrankedDocs = unrankedDocs;
      this.limit = limit;
    }

    @Override
    public Uni<RerankingTaskResult> get() {

      var passages =
          unrankedDocs.stream()
              .map(
                  scored ->
                      scored
                          .passage()
                          .orElseThrow(
                              () ->
                                  new IllegalArgumentException(
                                      "ScoredDocument passage isEmpty() document=%s"
                                          .formatted(scored))))
              .toList();

      if (passages.isEmpty()) {
        // avoid making a call we don't need to
        requestTracing.maybeTrace(
            () ->
                new TraceMessage(
                    "Reranking call skipped because 0 passages to rerank using %s with model %s"
                        .formatted(
                            classSimpleName(rerankingProvider.getClass()),
                            rerankingProvider.modelName()),
                    Recordable.copyOf(
                        Map.of(
                            "query", query,
                            "limit", limit,
                            "passages", passages))));

        return Uni.createFrom()
            .item(
                RerankingTaskResult.create(
                    requestTracing,
                    rerankingProvider,
                    new RerankingProvider.RerankingResponse(List.of()),
                    unrankedDocs,
                    limit));
      }

      requestTracing.maybeTrace(
          () ->
              new TraceMessage(
                  "Reranking %s passages using %s with model %s"
                      .formatted(
                          unrankedDocs.size(),
                          classSimpleName(rerankingProvider.getClass()),
                          rerankingProvider.modelName()),
                  Recordable.copyOf(
                      Map.of(
                          "query", query,
                          "limit", limit,
                          "passages", passages))));

      return rerankingProvider
          .rerank(query, passages, credentials)
          .onItem()
          .transform(
              rerankingResponse ->
                  RerankingTaskResult.create(
                      requestTracing, rerankingProvider, rerankingResponse, unrankedDocs, limit));
    }
  }

  /**
   * Merges the results of the ReRanking call with the unranked documents, ranks and enforces limit
   *
   * <p>
   */
  public static class RerankingTaskResult {
    private static final Logger LOGGER = LoggerFactory.getLogger(RerankingTaskResult.class);

    private final List<ScoredDocument> rerankedDocuments;

    private RerankingTaskResult(List<ScoredDocument> rerankedDocuments) {
      this.rerankedDocuments = rerankedDocuments;
    }

    /** */
    static RerankingTaskResult create(
        RequestTracing requestTracing,
        RerankingProvider rerankingProvider,
        RerankingProvider.RerankingResponse rerankingResponse,
        List<ScoredDocument> unrankedDocuments,
        int limit) {

      requestTracing.maybeTrace(
          () ->
              new TraceMessage(
                  "Processing reranking response with %s scores using %s with model %s"
                      .formatted(
                          rerankingResponse.ranks().size(),
                          classSimpleName(rerankingProvider.getClass()),
                          rerankingProvider.modelName()),
                  Recordable.copyOf(Map.of("scores", rerankingResponse.ranks()))));

      // in a factory to avoid too much work in a ctor
      var ranks = rerankingResponse.ranks();
      if (unrankedDocuments.size() != ranks.size()) {
        throw new IllegalArgumentException("unrankedDocuments and ranks must be the same size");
      }

      List<ScoredDocument> rerankedDocuments = new ArrayList<>(unrankedDocuments.size());
      for (var rank : ranks) {

        var rerankScore = DocumentScores.fromReranking(rank.score());
        ScoredDocument unrankedDoc;
        try {
          unrankedDoc = unrankedDocuments.get(rank.index());
        } catch (IndexOutOfBoundsException e) {
          throw new IllegalArgumentException(
              "rank index %s out of bounds for unrankedDocuments.size()=%s"
                  .formatted(rank.index(), unrankedDocuments.size()));
        }

        // merge the scores, this will keep the vector score if there was one
        unrankedDoc.mergeScore(rerankScore);
        rerankedDocuments.add(unrankedDoc);
      }

      rerankedDocuments.sort(Comparator.naturalOrder());
      var truncatedDocuments =
          List.copyOf(rerankedDocuments).subList(0, Math.min(rerankedDocuments.size(), limit));
      LOGGER.debug(
          "create() - documents reranked and truncated, unrankedDocuments.size()={}, limit={}, truncatedDocuments.size()={}",
          unrankedDocuments.size(),
          limit,
          truncatedDocuments.size());

      return new RerankingTaskResult(truncatedDocuments);
    }

    public List<ScoredDocument> rerankedDocuments() {
      return rerankedDocuments;
    }
  }
}
