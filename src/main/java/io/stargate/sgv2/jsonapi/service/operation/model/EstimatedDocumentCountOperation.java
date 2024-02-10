package io.stargate.sgv2.jsonapi.service.operation.model;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
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

    QueryOuterClass.Query query = null;
    query =
        new QueryBuilder()
            .select()
            .star()
            .from("system", "size_estimates")
            .where("keyspace_name", Predicate.EQ, commandContext.namespace())
            .where("table_name", Predicate.EQ, commandContext.collection())
            .build();

    final SimpleStatement simpleStatement = SimpleStatement.newInstance(query.getCql());
    return simpleStatement;
  }
}
