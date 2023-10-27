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
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CountOperationPage;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ExpressionBuilder;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.JsonTerm;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
    List<Expression<BuiltCondition>> expressions =
        ExpressionBuilder.buildExpressions(logicalExpression, null);
    Set<BuiltCondition> conditions = new LinkedHashSet<>();
    if (expressions != null && !expressions.isEmpty() && expressions.get(0) != null)
      expressions.get(0).collectK(conditions, Integer.MAX_VALUE);
    final List<Object> collect =
        conditions.stream()
            .flatMap(
                builtCondition -> {
                  JsonTerm term = ((JsonTerm) builtCondition.value());
                  List<Object> values = new ArrayList<>();
                  if (term.getKey() != null) values.add(term.getKey());
                  values.add(term.getValue());
                  return values.stream();
                })
            .collect(Collectors.toList());
    final QueryOuterClass.Query query =
        new QueryBuilder()
            .select()
            .count()
            .as("count")
            .from(commandContext.namespace(), commandContext.collection())
            .where(expressions.get(0)) // TODO count will assume no id filter query split?
            .build();

    final SimpleStatement simpleStatement = SimpleStatement.newInstance(query.getCql());
    return simpleStatement.setPositionalValues(collect);
  }
}
