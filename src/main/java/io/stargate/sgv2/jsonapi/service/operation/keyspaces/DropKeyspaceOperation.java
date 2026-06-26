package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.collections.SchemaChangeResult;
import java.util.function.Supplier;

public record DropKeyspaceOperation(CqlIdentifier name) implements Operation {

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    return queryExecutor
        .executeDropSchemaChange(dataApiRequestInfo, buildStatement())
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }

  private SimpleStatement buildStatement() {
    return SchemaBuilder.dropKeyspace(name).ifExists().build();
  }
}
