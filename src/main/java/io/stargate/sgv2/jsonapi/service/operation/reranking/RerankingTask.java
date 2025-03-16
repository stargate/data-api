package io.stargate.sgv2.jsonapi.service.operation.reranking;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.RERANK_FIELD;
import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.ResponseData;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RerankingTask<SchemaT extends TableBasedSchemaObject>
    extends BaseTask<
        SchemaT, RerankingTask.RerankingResultSupplier, RerankingTask.RerankingTaskResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RerankingTask.class);

  private final Object rerankingProvider;
  private final List<DeferredCommandResult> deferredReads;

  private float[] sortVector = null;
  // captured in onSuccess
  private RerankingTaskResult rerankingTaskResult;

  public RerankingTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      Object rerankingProvider,
      List<DeferredCommandResult> deferredReads) {
    super(position, schemaObject, retryPolicy);

    this.rerankingProvider = rerankingProvider;
    this.deferredReads = deferredReads;

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

    // If we are being called to run, the deferred reads we were waiting for should be completed.
    // This will throw if that is not the case.
    List<CommandResult> rawReadResults =
        deferredReads.stream().map(DeferredCommandResult::commandResult).toList();

    // Find the sort vector that was used for the inner query, if any
    // For now there should only ever be one sort vector included
    for (CommandResult rawReadResult : rawReadResults) {
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

    var dedupResult = deduplicateResults(rawReadResults);

    commandContext
        .requestTracing()
        .maybeTrace(
            "De-duplicated %s documents from inner reads to %s documents for reranking"
                .formatted(dedupResult.totalDocuments(), dedupResult.deduplicatedDocuments.size()));

    return new RerankingResultSupplier(dedupResult.deduplicatedDocuments());
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
    return super.recordTo(dataRecorder);
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

  private record DeduplicationResult(
      int totalDocuments, List<ScoredDocument> deduplicatedDocuments) {}

  private DeduplicationResult deduplicateResults(List<CommandResult> rawReadResults) {

    // This code relies on working with collections where the documents all have _id
    // will need to change for tables and rows
    Set<JsonNode> seenDocs = new HashSet<>();
    List<ScoredDocument> deduplicatedDocuments =
        new ArrayList<>(Math.divideExact(rawReadResults.size(), 2));
    int totalCount = 0;

    for (var commandResult : rawReadResults) {
      var multiDocResponse = (ResponseData.MultiResponseData) commandResult.data();
      totalCount += multiDocResponse.documents().size();
      for (var doc : multiDocResponse.documents()) {
        if (!seenDocs.add(doc.get(DocumentConstants.Fields.DOC_ID))) {
          var similarityNode = doc.get(DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD);
          if (similarityNode != null && !similarityNode.isNumber()) {
            throw new IllegalStateException(
                "%s document field is not a number for doc _id %s"
                    .formatted(
                        DocumentConstants.Fields.VECTOR_FUNCTION_SIMILARITY_FIELD,
                        doc.get(DocumentConstants.Fields.DOC_ID)));
          }
          var score =
              similarityNode == null
                  ? DocumentScore.empty()
                  : DocumentScore.vectorScore(similarityNode.floatValue());
          deduplicatedDocuments.add(new ScoredDocument(doc, score));
        }
      }
    }
    return new DeduplicationResult(totalCount, deduplicatedDocuments);
  }

  /** Calls the reranking provider to get the ranking */
  public static class RerankingResultSupplier implements UniSupplier<RerankingTaskResult> {

    private final List<ScoredDocument> unrankedDocs;

    RerankingResultSupplier(List<ScoredDocument> unrankedDocs) {
      this.unrankedDocs = unrankedDocs;
    }

    @Override
    public Uni<RerankingTaskResult> get() {

      // TODO: Call the reranking provider and handle the results
      List<FakeRank> fakeRanks = new ArrayList<>(unrankedDocs.size());
      Random random = new Random();
      for (int i = 0; i < unrankedDocs.size(); i++) {
        fakeRanks.add(new FakeRank(i, random.nextFloat()));
      }
      return Uni.createFrom().item(RerankingTaskResult.create(unrankedDocs, fakeRanks));
    }
  }

  /** Merges the results of the ReRanking call with the unranked documents, and ranks them */
  public static class RerankingTaskResult {

    private final List<ScoredDocument> rerankedDocuments;

    private RerankingTaskResult(List<ScoredDocument> rerankedDocuments) {
      this.rerankedDocuments = rerankedDocuments;
    }

    /** */
    static RerankingTaskResult create(
        List<ScoredDocument> unrankedDocuments, List<FakeRank> fakeRanks) {
      // in a factory to avoid too much work in a ctor

      if (unrankedDocuments.size() != fakeRanks.size()) {
        throw new IllegalArgumentException("unrankedDocuments and fakeRanks must be the same size");
      }

      List<ScoredDocument> rerankedDocuments = new ArrayList<>(unrankedDocuments.size());
      for (var fakeRank : fakeRanks) {

        var rerankScore = DocumentScore.rerankScore(fakeRank.rank());
        ScoredDocument unrankedDoc;
        try {
          unrankedDoc = unrankedDocuments.get(fakeRank.index());
        } catch (IndexOutOfBoundsException e) {
          throw new IllegalArgumentException(
              "fakeRank index %s out of bounds for unrankedDocuments.size()=%s"
                  .formatted(fakeRank.index(), unrankedDocuments.size()));
        }

        // merge the scores, this will keep the vector score if there was one
        rerankedDocuments.add(
            new ScoredDocument(unrankedDoc.document(), unrankedDoc.score().merge(rerankScore)));
      }
      rerankedDocuments.sort(Comparator.naturalOrder());
      return new RerankingTaskResult(List.copyOf(rerankedDocuments));
    }

    public List<ScoredDocument> rerankedDocuments() {
      return rerankedDocuments;
    }
  }

  record FakeRank(int index, float rank) {}

  public record DocumentScore(
      @JsonIgnore boolean hasRerank,
      @JsonProperty(RERANK_FIELD) float rerank,
      @JsonIgnore boolean hasVector,
      @JsonProperty(VECTOR_EMBEDDING_FIELD) float vector)
      implements Comparable<DocumentScore> {
    private static final DocumentScore EMPTY = new DocumentScore(false, 0, false, 0);

    static DocumentScore empty() {
      return EMPTY;
    }

    static DocumentScore vectorScore(float vector) {
      return new DocumentScore(false, 0, true, vector);
    }

    static DocumentScore rerankScore(float rerank) {
      return new DocumentScore(true, rerank, false, 0);
    }

    DocumentScore merge(DocumentScore other) {
      if (hasRerank && other.hasRerank) {
        throw new IllegalArgumentException("Cannot merge two rerank scores");
      }
      if (hasVector && other.hasVector) {
        throw new IllegalArgumentException("Cannot merge two vector scores");
      }
      return new DocumentScore(
          hasRerank || other.hasRerank,
          hasRerank ? rerank : other.rerank,
          hasVector || other.hasVector,
          hasVector ? vector : other.vector);
    }

    /**
     * Sorts based on the rerank score, other scores are ignored.
     *
     * <p>NOTE: this sorts in descending order, larger scores are better
     */
    @Override
    public int compareTo(DocumentScore o) {
      if (!hasRerank && !o.hasRerank) {
        throw new IllegalStateException(
            "Attempt to compare two DocumentScores where one does not have a rerank score");
      }
      return Float.compare(rerank, o.rerank) * -1;
    }
  }
  ;

  public record ScoredDocument(JsonNode document, DocumentScore score)
      implements Comparable<ScoredDocument> {
    @Override
    public int compareTo(ScoredDocument o) {
      return score.compareTo(o.score);
    }
  }
  ;
}
