package io.stargate.sgv2.jsonapi.service.resolver;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/**
 * Base for a class that can resolve a clause in a command for use by an Operation.
 * <p>
 * Clauses are things like the filter or update clause.
 * <p>
 * The end goal for the design is that a {@link CommandResolver} uses the {@link ClauseResolver} 's to create
 * subtypes of the {@link io.stargate.sgv2.jsonapi.service.operation.query.CQLClause} for
 * the {@link io.stargate.sgv2.jsonapi.service.operation.Operation} to use. TODO: we cannot enforce that it returns a
 * CQLClause the the collections break the achitecture and pass the {@link io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression} down to the Operation.
 * <p>
 * Because the {@link io.stargate.sgv2.jsonapi.service.operation.query.CQLClause} implementations are Java functions
 * we can also use multiple resolvers for a clause and then chain the CQL clauses.
 * <p>
 * @param <CmdT> The type of the command this resolver can resolve.
 * @param <SchemaT> The type of the schema object the command is operating on.
 * @param <R> The type of the response from the resolver.
 */
public abstract class ClauseResolver<CmdT extends Command, SchemaT extends SchemaObject, R> {

  protected final OperationsConfig operationsConfig;

  protected ClauseResolver(OperationsConfig operationsConfig) {
    Preconditions.checkNotNull(operationsConfig, "operationsConfig is required");
    this.operationsConfig = operationsConfig;
  }

  public abstract R resolve(CommandContext<SchemaT> commandContext, CmdT command);
}
