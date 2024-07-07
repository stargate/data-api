package io.stargate.sgv2.jsonapi.service.operation.model.collections;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

/** Operation that returns estimated count of documents. */
public record EstimatedDocumentCountOperation<T extends SchemaObject>(
    CommandContext<T> commandContext) implements CollectionReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    SimpleStatement simpleStatement = buildSelectQuery();
    Uni<CountResponse> countResponse =
        estimateDocumentCount(dataApiRequestInfo, queryExecutor, simpleStatement);

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
        .isEqualTo(literal(commandContext.schemaObject().name.keyspace()))
        .whereColumn("table_name")
        .isEqualTo(literal(commandContext.schemaObject().name.table()))
        .build();
  }
}
