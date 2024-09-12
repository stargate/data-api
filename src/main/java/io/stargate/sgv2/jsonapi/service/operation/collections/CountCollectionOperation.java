package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cql.builder.Query;
import io.stargate.sgv2.jsonapi.service.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import java.util.List;
import java.util.function.Supplier;

/**
 * Operation that returns count of documents based on the filter condition. Written with the
 * assumption that all variables to be indexed.
 */
public record CountCollectionOperation(
    CommandContext commandContext, DBLogicalExpression dbLogicalExpression, int pageSize, int limit)
    implements CollectionReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    SimpleStatement simpleStatement = buildSelectQuery();
    Uni<CountResponse> countResponse = null;
    if (limit == -1)
      countResponse = countDocuments(dataApiRequestInfo, queryExecutor, simpleStatement);
    else countResponse = countDocumentsByKey(dataApiRequestInfo, queryExecutor, simpleStatement);

    return countResponse
        .onItem()
        .transform(
            docs -> {
              if (limit == -1) {
                return new CountOperationPage(docs.count(), false);
              } else {
                boolean moreData = docs.count() > limit();
                return new CountOperationPage(
                    docs.count() > limit() ? docs.count() - 1 : docs.count(), moreData);
              }
            });
  }

  private SimpleStatement buildSelectQuery() {
    final List<Expression<BuiltCondition>> expressions =
        ExpressionBuilder.buildExpressions(dbLogicalExpression, null);
    Query query = null;
    if (limit == -1) {
      query =
          new QueryBuilder()
              .select()
              .count()
              .as("count")
              .from(
                  commandContext.schemaObject().name().keyspace(),
                  commandContext.schemaObject().name().table())
              .where(expressions.get(0))
              .build();
    } else {
      query =
          new QueryBuilder()
              .select()
              .column("key")
              .from(
                  commandContext.schemaObject().name().keyspace(),
                  commandContext.schemaObject().name().table())
              .where(expressions.get(0))
              .limit(limit + 1)
              .build();
    }
    SimpleStatement simpleStatement = query.queryToStatement();
    simpleStatement.setPageSize(pageSize());
    return simpleStatement;
  }
}
