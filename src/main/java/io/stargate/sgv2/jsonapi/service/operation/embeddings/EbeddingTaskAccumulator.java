package io.stargate.sgv2.jsonapi.service.operation.embeddings;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.CompositeTask;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskAccumulator;
import java.util.function.Supplier;

/**
 * Accumulates the {@link EmbeddingTask}s as they complete processing, and when getResults() is
 * called it sets of the vectors using the {@link EmbeddingAction}
 *
 * @param <SchemaT>
 */
public class EbeddingTaskAccumulator<SchemaT extends TableBasedSchemaObject>
    extends TaskAccumulator<EmbeddingTask<SchemaT>, SchemaT> {

  EbeddingTaskAccumulator() {}

  @Override
  public Supplier<CommandResult> getResults() {
    return CompositeTask.NULL_COMMAND_MARKER;
  }
}
