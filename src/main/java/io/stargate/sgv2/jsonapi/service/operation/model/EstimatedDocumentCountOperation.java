package io.stargate.sgv2.jsonapi.service.operation.model;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.EstimatedCountResult;
import java.util.function.Supplier;

/** Operation that returns estimated count of documents. */
public record EstimatedDocumentCountOperation(CommandContext commandContext)
    implements ReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    SimpleStatement simpleStatement = buildSelectQuery();
    Uni<CountResponse> countResponse = estimateDocumentCount(queryExecutor, simpleStatement);

    return countResponse
        .onItem()
        .transform(
            docs -> {
              return new EstimatedCountResult(docs.count());
            });
  }

  private SimpleStatement buildSelectQuery() {
    return selectFrom("system", "size_estimates")
        .all()
        .whereColumn("keyspace_name")
        .isEqualTo(literal(commandContext.namespace()))
        .whereColumn("table_name")
        .isEqualTo(literal(commandContext.collection()))
        .build();
  }
}
