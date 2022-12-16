package io.stargate.sgv3.docsapi.service;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.operations.Operation;
import io.stargate.sgv3.docsapi.operations.OperationResult;
import javax.enterprise.context.ApplicationScoped;

/**
 * Executes the {@link Operation} with appropriate resources.
 *
 * <p>May provide a thread or resource boundary from the calling Command layer. Can make decisions
 * like running pushdown operations in one pool, and filtering operations in another. This would
 * also be responsible for killing operations that take too long.
 *
 * <p>The executor maintains and provides the db connection etc in the {@link ExecutionState} and
 * provides that to the operation to run.
 */
@ApplicationScoped
public class OperationExecutor implements AutoCloseable {

  private final QueryExecutor queryExecutor;

  public OperationExecutor(QueryExecutor queryExecutor) {
    this.queryExecutor = queryExecutor;
  }

  public Uni<OperationResult> executeOperation(Operation operation) {
    // This is the place to look at the operation and decide the resources we want to run it.
    // .e.g if this operation requires memory and CPU for filtering that may be different to
    // select by ID which is quick and fully push down.
    Uni<OperationResult> result = operation.execute(queryExecutor);
    return result
        .onItem()
        .ifNull()
        .failWith(
            new RuntimeException(String.format("Null returned from the operation %s", operation)));
  }

  @Override
  public void close() throws Exception {}
}
