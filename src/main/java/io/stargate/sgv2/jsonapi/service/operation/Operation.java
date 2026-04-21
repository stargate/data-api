package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.collections.CollectionDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.databases.DatabaseDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.KeyspaceDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import java.util.function.Function;
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

    // amorton - gh#2318 - this method is only used by the legacy collection code which will be
    // removed.
    // all the table code is in tasks, see TaskOperation.execute(CommandContext)
    // with that in mind, we can check/force this is a collection code path

    Function<SimpleStatement, DriverExceptionHandler> exceptionHandlerFactory =
        switch (commandContext.schemaObject().type()) {
          case COLLECTION ->
              statement ->
                  new CollectionDriverExceptionHandler(
                      commandContext.asCollectionContext().schemaObject(), statement);
          case KEYSPACE ->
              statement ->
                  new KeyspaceDriverExceptionHandler(
                      commandContext.asKeyspaceContext().schemaObject(), statement);
          case DATABASE ->
              statement ->
                  new DatabaseDriverExceptionHandler(
                      commandContext.asDatabaseContext().schemaObject(), statement);
          default ->
              throw new UnsupportedOperationException(
                  "Unexpected schema type for legacy DB operation: "
                      + commandContext.schemaObject().type());
        };

    return execute(
        commandContext.requestContext(),
        new QueryExecutor(
            commandContext.cqlSessionCache(),
            commandContext.config().get(OperationsConfig.class),
            exceptionHandlerFactory,
            commandContext.requestTracing()));
  }
}
