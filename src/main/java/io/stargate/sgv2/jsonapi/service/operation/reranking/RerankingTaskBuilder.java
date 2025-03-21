package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TaskBuilder;

public class RerankingTaskBuilder<SchemaT extends TableBasedSchemaObject>
    extends TaskBuilder<RerankingTask<SchemaT>, SchemaT, RerankingTaskBuilder<SchemaT>> {

  public RerankingTaskBuilder(CommandContext<SchemaT> commandContext) {
    super(commandContext.schemaObject());
  }
}
