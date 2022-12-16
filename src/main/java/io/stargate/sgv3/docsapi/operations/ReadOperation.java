package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.exception.DocumentReadException;

/** Super class for operations that read data without changing it. */
public abstract class ReadOperation extends Operation {

  protected ReadOperation(CommandContext commandContext) {
    super(commandContext);
  }

  @Override
  public Uni<OperationResult> execute(QueryExecutor queryExecutor) {
    Uni<ReadOperationPage> page = executeInternal(queryExecutor);
    return page.onFailure()
        .transform(t -> new DocumentReadException(t))
        .onItem()
        .transform(p -> p.createOperationResult());
  }

  /**
   * Implementors should do all the work to run the Operation in this function, and return
   * information on what was changed.
   *
   * <p>Implementors should not handle any database errors, they will be handled by execute()
   *
   * @param queryExecutor
   * @return
   */
  protected abstract Uni<ReadOperationPage> executeInternal(QueryExecutor queryExecutor);
}
