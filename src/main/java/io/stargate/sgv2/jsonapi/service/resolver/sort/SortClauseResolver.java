package io.stargate.sgv2.jsonapi.service.resolver.sort;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.resolver.ClauseResolver;

/**
 * Base for classes that resolve a sort clause on a {@link Command} into something the Operation can
 * use to add <code>ORDER BY</code> to a query.
 *
 * <p>NOTE: this is for adding the CQL order by, while still do some in memory sorting for CQL that
 * is handled differently.
 *
 * @param <CmdT> The type of the {@link Command} that is being resolved.
 * @param <SchemaT> The type of the {@link SchemaObject} that {@link Command} command is operating
 *     on.
 */
public abstract class SortClauseResolver<
        CmdT extends Command & Sortable, SchemaT extends SchemaObject>
    extends ClauseResolver<CmdT, SchemaT, OrderByCqlClause> {

  protected SortClauseResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }
}
