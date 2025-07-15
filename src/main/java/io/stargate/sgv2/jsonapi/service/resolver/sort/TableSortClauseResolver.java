package io.stargate.sgv2.jsonapi.service.resolver.sort;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.resolver.ClauseResolver;

/**
 * Common base for common code when resolving the sort clause for either CQL or in memory sorting
 *
 * @param <CmdT> The command type
 * @param <SchemaT> The schema object type
 * @param <ReturnT> The type that the resolver returns, e.g. in memory or cql sorting
 */
public abstract class TableSortClauseResolver<
        CmdT extends Command & Sortable, SchemaT extends SchemaObject, ReturnT>
    extends ClauseResolver<CmdT, SchemaT, ReturnT> {

  protected TableSortClauseResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }
}
