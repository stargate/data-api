package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.service.OperationExecutor;
import java.util.List;

/**
 * Operations get data in and out of Cassandra. Some are very specific by find on doc by ID, or
 * insert one doc. Others may be more general like filtering by several text fields.
 *
 * <p>Operations may be able to push down to the database (this is good) but some will may need to
 * filter or sort in memory (not as good). In general good things are good.
 *
 * <p>Operations are built with all the data they need to execute, and only know about standard Java
 * types and our shredded document. That is they do not interact with JSON. As well as data they
 * also have all the behavior they need to implement the operation.
 *
 * <p>We execute the operation with consideration for it's costs, e.g. we may want to run pushdown
 * operations in a different pool to non-pushdown. An approach for this is illustrated in the {@link
 * OperationPlan}.
 *
 * <p>Decisions about where and how to execute an operation are made in the {@link
 * OperationExecutor}
 */
public abstract class Operation {

  /** {@link CommandContext} has the tenant, user, database, table etc to say where to run */
  private CommandContext commandContext;

  protected Operation(CommandContext commandContext) {
    this.commandContext = commandContext;
  }

  public CommandContext getCommandContext() {
    return commandContext;
  }

  /**
   * Single entry point to run the operation and get it's result.
   *
   * <p>Will be called from {@link OperationExecutor}
   *
   * @param execState The DB resources we are going to use to execute this command.
   * @return
   */
  public abstract Uni<OperationResult> execute(QueryExecutor queryExecutor);

  /**
   * Centralized handling of errors raised when execute() is called.
   *
   * <p>Implementors should call this when they get an error.
   *
   * @param e
   * @return
   */
  protected OperationResult handleExecutionError(Exception e) {
    return OperationResult.builder().withErrors(List.of(e)).build();
  }

  /**
   * Returns a {@link OperationPlan} that the executor uses to decide how to run the operation.
   *
   * @return
   */
  public abstract OperationPlan getPlan();
}
