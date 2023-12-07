package io.stargate.sgv2.jsonapi.service.operation.model;

import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CountOperationPage;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ExpressionBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Operation that returns count of documents based on the filter condition. Written with the
 * assumption that all variables to be indexed.
 */
public record CountOperation(CommandContext commandContext, LogicalExpression logicalExpression)
    implements ReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    SimpleStatement simpleStatement = buildSelectQuery();
    return countDocuments(queryExecutor, simpleStatement)
        .onItem()
        .transform(docs -> new CountOperationPage(docs.count()));
  }

  private SimpleStatement buildSelectQuery() {

    final ExpressionBuilder.ExpressionBuiltResult expressionBuiltResult =
        ExpressionBuilder.buildExpressions(logicalExpression, null);
    final List<Expression<BuiltCondition>> expressions = expressionBuiltResult.expressions();
    List<Object> collect = new ArrayList<>();
    if (expressions != null && !expressions.isEmpty() && expressions.get(0) != null) {
      collect = ExpressionBuilder.getExpressionValuesInOrder(expressions.get(0));
    }
    final QueryOuterClass.Query query =
        new QueryBuilder()
            .select()
            .count()
            .as("count")
            .from(commandContext.namespace(), commandContext.collection())
            .where(expressions.get(0)) // TODO count will assume no id filter query split?
            .allowFiltering(expressionBuiltResult.allowFiltering())
            .build();

    final SimpleStatement simpleStatement = SimpleStatement.newInstance(query.getCql());
    return simpleStatement.setPositionalValues(collect);
  }
}
