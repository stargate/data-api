package io.stargate.sgv2.jsonapi.service.resolver.sort;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.Updatable;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.OrderByCqlClause;
import io.stargate.sgv2.jsonapi.service.operation.query.UpdateValuesCQLClause;
import io.stargate.sgv2.jsonapi.service.resolver.ClauseResolver;

public abstract class SortClauseResolver<CmdT extends Command & Sortable, SchemaT extends SchemaObject>
    extends ClauseResolver<CmdT, SchemaT, OrderByCqlClause> {

  protected SortClauseResolver(OperationsConfig operationsConfig){
    super(operationsConfig);
  }
}
