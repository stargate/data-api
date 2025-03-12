package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RerankingTask<SchemaT extends TableBasedSchemaObject>
    extends BaseTask<
        SchemaT, RerankingTask.RerankingResultSupplier, RerankingTask.RerankingTaskResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RerankingTask.class);

  private final Object rerankingProvider;
  private final List<DeferredCommandResult> intermediateReads;

  public RerankingTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      Object rerankingProvider,
      List<DeferredCommandResult> intermediateReads) {
    super(position, schemaObject, retryPolicy);

    this.rerankingProvider = rerankingProvider;
    this.intermediateReads = intermediateReads;

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
  protected RerankingResultSupplier buildResultSupplier(
      CommandContext<SchemaT> commandContext) {

    return null;
  }

  @Override
  protected RuntimeException maybeHandleException(
      RerankingResultSupplier resultSupplier, RuntimeException runtimeException) {
    return runtimeException;
  }


  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder);
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  public static class RerankingResultSupplier implements UniSupplier<RerankingTaskResult> {

    RerankingResultSupplier() {
    }

    @Override
    public Uni<RerankingTaskResult> get() {

      return null;
    }
  }

  public static class RerankingTaskResult {


    RerankingTaskResult() {
    }

  }
}
