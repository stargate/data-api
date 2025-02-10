package io.stargate.sgv2.jsonapi.service.operation;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

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
 */
public interface Operation<SchemaT extends SchemaObject> {

  /** Use {@link #execute(CommandContext)} this is for original legacy operations */
  @Deprecated
  default Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    throw new UnsupportedOperationException(
        "execute(RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) is a legacy method and should not be called.");
  }

  /**
   * Execute the operation with the provided {@link CommandContext}.
   *
   * <p>The default implementation calls the legacy method {@link #execute(RequestContext,
   * QueryExecutor)} new implementations should override this method legacy can override {@link
   * #execute(RequestContext, QueryExecutor)}.
   */
  default Uni<Supplier<CommandResult>> execute(CommandContext<SchemaT> commandContext) {
    return execute(
        commandContext.requestContext(),
        new QueryExecutor(
            commandContext.cqlSessionCache(), commandContext.config().get(OperationsConfig.class)));
  }
}
