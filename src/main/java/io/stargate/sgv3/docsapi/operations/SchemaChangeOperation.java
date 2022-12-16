package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;

public abstract class SchemaChangeOperation extends Operation {

  protected SchemaChangeOperation(CommandContext commandContext) {
    super(commandContext);
  }

  @Override
  public Uni<OperationResult> execute(QueryExecutor queryExecutor) {
    Uni<SchemaChangeResult> result = executeInternal(queryExecutor);
    return result.onItem().transform(res -> res.createOperationResult());
  }

  protected abstract Uni<SchemaChangeResult> executeInternal(QueryExecutor queryExecutor);
}
