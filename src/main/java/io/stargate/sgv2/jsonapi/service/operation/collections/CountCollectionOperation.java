package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cql.builder.Query;
import io.stargate.sgv2.jsonapi.service.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.OpenSearchCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import java.util.List;
import java.util.function.Supplier;

/**
 * Operation that returns count of documents based on the filter condition. Written with the
 * assumption that all variables to be indexed.
 *
 * <p>When the {@link DBLogicalExpression} contains an {@link OpenSearchCollectionFilter} the count
 * is resolved via {@code SELECT COUNT(*) … WHERE expr(<saiIndexName>, ?)} so that HCD's {@code
 * OpenSearchQueryHandler} intercepts it and returns the OpenSearch {@code numFound} value — rather
 * than doing a full Cassandra row-key scan.
 */
public record CountCollectionOperation(
    CommandContext commandContext, DBLogicalExpression dbLogicalExpression, int pageSize, int limit)
    implements CollectionReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext dataApiRequestInfo, QueryExecutor queryExecutor) {
    SimpleStatement simpleStatement = buildSelectQuery();
    Uni<CountResponse> countResponse;

    // When the filter contains an OpenSearch expr(), HCD's OpenSearchQueryHandler intercepts
    // SELECT COUNT(*) and returns the OpenSearch numFound value via the normal CQL count row.
    // Always use the COUNT(*) path for OpenSearch filters; the key-scan path is irrelevant.
    if (limit == -1 || hasOpenSearchFilter(dbLogicalExpression))
      countResponse = countDocuments(dataApiRequestInfo, queryExecutor, simpleStatement);
    else countResponse = countDocumentsByKey(dataApiRequestInfo, queryExecutor, simpleStatement);

    return countResponse
        .onItem()
        .transform(
            docs -> {
              if (limit == -1 || hasOpenSearchFilter(dbLogicalExpression)) {
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
    Query query;
    // For OpenSearch filters always emit SELECT COUNT(*) — HCD routes it to OS numFound.
    if (limit == -1 || hasOpenSearchFilter(dbLogicalExpression)) {
      query =
          new QueryBuilder()
              .select()
              .count()
              .as("count")
              .from(
                  commandContext.schemaObject().identifier().keyspace(),
                  commandContext.schemaObject().identifier().table())
              .where(expressions.getFirst())
              .build();
    } else {
      query =
          new QueryBuilder()
              .select()
              .column("key")
              .from(
                  commandContext.schemaObject().identifier().keyspace(),
                  commandContext.schemaObject().identifier().table())
              .where(expressions.getFirst())
              .limit(limit + 1)
              .build();
    }
    SimpleStatement simpleStatement = query.queryToStatement();
    // SimpleStatement is immutable, setPageSize returns a new statement
    return simpleStatement.setPageSize(pageSize());
  }

  /**
   * Returns {@code true} if {@code expr} (recursively) contains an {@link
   * OpenSearchCollectionFilter}.
   */
  private static boolean hasOpenSearchFilter(DBLogicalExpression expr) {
    for (DBFilterBase filter : expr.filters()) {
      if (filter instanceof OpenSearchCollectionFilter) {
        return true;
      }
    }
    for (DBLogicalExpression sub : expr.subExpressions()) {
      if (hasOpenSearchFilter(sub)) {
        return true;
      }
    }
    return false;
  }
}
