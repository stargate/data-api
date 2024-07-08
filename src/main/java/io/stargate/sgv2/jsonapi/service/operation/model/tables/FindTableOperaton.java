package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import java.util.function.Supplier;
import org.apache.commons.lang3.NotImplementedException;

public class FindTableOperaton extends TableReadOperation {

  public FindTableOperaton(
      CommandContext<CollectionSchemaObject> commandContext, LogicalExpression logicalExpression) {
    super(commandContext, logicalExpression);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    throw new NotImplementedException("Placeholder - work in progress");
  }
}
