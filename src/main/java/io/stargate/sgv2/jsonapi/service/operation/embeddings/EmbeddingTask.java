package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.TraceMessage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.operation.tasks.BaseTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskRetryPolicy;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link io.stargate.sgv2.jsonapi.service.operation.tasks.Task} that makes a single call to an
 * {@link EmbeddingProvider} to vectorize a list of texts.
 *
 * @param <SchemaT> Type of the schema object the task operates on.
 */
public class EmbeddingTask<SchemaT extends TableBasedSchemaObject>
    extends BaseTask<
        SchemaT, EmbeddingTask.EmbeddingResultSupplier, EmbeddingTask.EmbeddingTaskResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddingTask.class);

  private final EmbeddingProvider embeddingProvider;
  private final List<EmbeddingDeferredAction> embeddingActions;
  private final EmbeddingProvider.EmbeddingRequestType requestType;

  protected EmbeddingTask(
      int position,
      SchemaT schemaObject,
      TaskRetryPolicy retryPolicy,
      EmbeddingProvider embeddingProvider,
      List<EmbeddingDeferredAction> embeddingActions,
      EmbeddingProvider.EmbeddingRequestType requestType) {
    super(position, schemaObject, retryPolicy);

    this.embeddingProvider =
        Objects.requireNonNull(embeddingProvider, "embeddingProvider must not be null");
    // defensive copy - we rely on the order of this list to match the order of returned vectors
    this.embeddingActions =
        List.copyOf(Objects.requireNonNull(embeddingActions, "embeddingActions must not be null"));
    if (this.embeddingActions.isEmpty()) {
      throw new IllegalArgumentException("embeddingActions cannot be empty");
    }
    this.requestType = requestType;

    setStatus(TaskStatus.READY);
  }

  public static <SchemaT extends TableBasedSchemaObject> EmbeddingTaskBuilder<SchemaT> builder(
      CommandContext<SchemaT> commandContext) {
    return new EmbeddingTaskBuilder<>(commandContext);
  }

  // =================================================================================================
  // BaseTask overrides
  // =================================================================================================

  @Override
  protected EmbeddingTask.EmbeddingResultSupplier buildResultSupplier(
      CommandContext<SchemaT> commandContext) {

    var vectorizeTexts =
        embeddingActions.stream().map(EmbeddingDeferredAction::startEmbedding).toList();

    return new EmbeddingResultSupplier(
        this,
        commandContext,
        () ->
            embeddingProvider.vectorize(
                1, // always use 1, microbatching happens in the provider.
                vectorizeTexts,
                commandContext
                    .requestContext()
                    .getEmbeddingCredentialsSupplier()
                    .create(commandContext.requestContext(), embeddingProvider.getProviderConfig()),
                requestType),
        embeddingActions,
        vectorizeTexts);
  }

  @Override
  protected RuntimeException maybeHandleException(
      EmbeddingResultSupplier resultSupplier, RuntimeException runtimeException) {
    // return the same exception to say it was not handled
    return runtimeException;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder)
        .append("requestType", requestType)
        .append("embeddingAction.groupKey", embeddingActions.getFirst().groupKey());
  }

  // =================================================================================================
  // Implementation and internals
  // =================================================================================================

  /**
   * Makes the actual calls to the {@link EmbeddingProvider} and then builds the {@link
   * EmbeddingTaskResult}
   *
   * <p>NOTE: mus tbe static, or the generics get upset
   */
  public static class EmbeddingResultSupplier implements BaseTask.UniSupplier<EmbeddingTaskResult> {

    protected final EmbeddingTask<?> embeddingTask;
    protected final CommandContext<?> commandContext;
    protected final BaseTask.UniSupplier<EmbeddingProvider.Response> supplier;
    protected final List<EmbeddingDeferredAction> actions;
    private final List<String> vectorizeTexts;

    EmbeddingResultSupplier(
        EmbeddingTask<?> embeddingTask,
        CommandContext<?> commandContext,
        BaseTask.UniSupplier<EmbeddingProvider.Response> supplier,
        List<EmbeddingDeferredAction> actions,
        List<String> vectorizeTexts) {
      this.embeddingTask = embeddingTask;
      this.commandContext = commandContext;
      this.supplier = supplier;
      this.actions = actions;
      this.vectorizeTexts = vectorizeTexts;
    }

    @Override
    public Uni<EmbeddingTaskResult> get() {

      commandContext
          .requestTracing()
          .maybeTrace(
              () ->
                  new TraceMessage(
                      "Requesting %s vectors using %s to vectorize with '%s/%s' for task %s"
                          .formatted(
                              vectorizeTexts.size(),
                              embeddingTask.embeddingProvider.getClass().getSimpleName(),
                              actions.getFirst().groupKey().vectorizeDefinition().provider(),
                              actions.getFirst().groupKey().vectorizeDefinition().modelName(),
                              embeddingTask.taskDesc()),
                      Recordable.copyOf(vectorizeTexts)));

      // The EmbeddingProviders use EmbeddingProviderErrorMapper and turn errors from the providers
      // into Error V1 JsonApiException structure, this will be attached to the task if we let it
      // bubble
      // out of here.
      return supplier
          .get()
          .onItem()
          .transform(
              rawResult ->
                  EmbeddingTaskResult.create(embeddingTask, commandContext, rawResult, actions));
    }
  }

  /**
   * The result calling the Embedding Provider, this class is responsible for delivering the vectors
   * to the {@link EmbeddingDeferredAction} so they end up in the {@link
   * io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue}s waiting for them.
   */
  public static class EmbeddingTaskResult {

    protected final List<float[]> rawVectors;
    protected final List<EmbeddingDeferredAction> actions;

    private EmbeddingTaskResult(List<float[]> rawVectors, List<EmbeddingDeferredAction> actions) {
      this.rawVectors = rawVectors;
      this.actions = actions;
    }

    /**
     * Create a new {@link EmbeddingTaskResult} which involves passing the vectors returned from the
     * provider to the {@link EmbeddingDeferredAction}s so they can set the values into the deferred
     * {@link io.stargate.sgv2.jsonapi.service.shredding.CqlNamedValue}s.
     */
    static EmbeddingTaskResult create(
        EmbeddingTask<?> embeddingTask,
        CommandContext<?> commandContext,
        EmbeddingProvider.Response providerResponse,
        List<EmbeddingDeferredAction> actions) {

      commandContext
          .requestTracing()
          .maybeTrace(
              () -> {
                var msg =
                    "Received %s vectors using %s to vectorize with '%s/%s' for task %s"
                        .formatted(
                            providerResponse.embeddings().size(),
                            embeddingTask.embeddingProvider.getClass().getSimpleName(),
                            actions.getFirst().groupKey().vectorizeDefinition().provider(),
                            actions.getFirst().groupKey().vectorizeDefinition().modelName(),
                            embeddingTask.taskDesc());
                return new TraceMessage(msg, Recordable.copyOf(providerResponse.embeddings()));
              });

      // defensive to make sure the order cannot change
      var vectors = List.copyOf(providerResponse.embeddings());

      if (vectors.size() != actions.size()) {
        throw new IllegalStateException(
            "Size of returned vectors and waiting EmbeddingActions do not match, vectors.size()=%s, actions.size()=%s"
                .formatted(vectors.size(), actions.size()));
      }
      // we rely on the response vectors having the same order we passed them in
      // if an error bubbles out of there the task pipeline will attach it to the task
      for (int i = 0; i < actions.size(); i++) {
        actions.get(i).onSuccess(vectors.get(i));
      }

      return new EmbeddingTaskResult(vectors, actions);
    }
  }
}
