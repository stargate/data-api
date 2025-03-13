package io.stargate.sgv2.jsonapi.service.operation.reranking;

import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.ResponseData;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RerankingTask<SchemaT extends TableBasedSchemaObject>
    extends BaseTask<
        SchemaT, RerankingTask.RerankingResultSupplier, RerankingTask.RerankingTaskResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RerankingTask.class);

  private final Object rerankingProvider;
  private final List<DeferredCommandResult> deferredReads;

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

    List<JsonNode> rerankingDocuments = deduplicateResults(rawReadResults);

    // TODO: get the actual number of raw documents
    commandContext
        .requestTracing()
        .maybeTrace(
            "De-duplicated %s documents from inner reads to %s documents for reranking"
                .formatted(rerankingDocuments.size(), rerankingDocuments.size()));

    return new RerankingResultSupplier(rerankingDocuments);
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

  private List<JsonNode> deduplicateResults(List<CommandResult> rawReadResults) {
    // TODO - proper IMPLEMENTATION
    return rawReadResults.stream()
        .map(CommandResult::data)
        .map(data -> (ResponseData.MultiResponseData) data)
        .map(ResponseData.MultiResponseData::documents)
        .flatMap(Collection::stream)
        .toList();
  }

  public static class RerankingResultSupplier implements UniSupplier<RerankingTaskResult> {

    private final List<JsonNode> rerankingDocuments;

    RerankingResultSupplier(List<JsonNode> rerankingDocuments) {
      this.rerankingDocuments = rerankingDocuments;
    }

    @Override
    public Uni<RerankingTaskResult> get() {

      // TODO: Call the reranking provider and handle the results
      return Uni.createFrom().item(new RerankingTaskResult(rerankingDocuments));
    }
  }

  public static class RerankingTaskResult {

    private final List<JsonNode> rerankedDocuments;

    RerankingTaskResult(List<JsonNode> rerankedDocuments) {
      // TODO: add a static facory method here that will reorder using the results from the reranker
      this.rerankedDocuments = rerankedDocuments;
    }

    public List<JsonNode> rerankedDocuments() {
      return rerankedDocuments;
    }
  }
}
