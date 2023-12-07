package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Variable;
import io.quarkus.logging.Log;
import io.stargate.sgv2.api.common.cql.ExpressionUtils;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ExpressionBuilder {

  public static List<Expression<BuiltCondition>> buildExpressions(
      LogicalExpression logicalExpression, DBFilterBase.IDFilter additionalIdFilter) {
    // an empty filter should find everything
    if (logicalExpression.isEmpty() && additionalIdFilter == null) {
      return Collections.singletonList(null);
    }
    // after validate in FilterClauseDeserializer,
    // partition key column key will not be nested under OR operator
    // so we can collect all id_conditions, then do a combination to generate separate queries
    List<DBFilterBase.IDFilter> idFilters = new ArrayList<>();
    // expressionWithoutId must be a And(if not null)
    // since we have outer implicit and in the filter
    Expression<BuiltCondition> expressionWithoutId =
        buildExpressionRecursive(logicalExpression, additionalIdFilter, idFilters);
    List<Expression<BuiltCondition>> expressions =
        buildExpressionWithId(additionalIdFilter, expressionWithoutId, idFilters);

    Log.error("expression !! " + expressions);
    return expressions;
  }

  private static List<Expression<BuiltCondition>> buildExpressionWithId(
      DBFilterBase.IDFilter additionalIdFilter,
      Expression<BuiltCondition> expressionWithoutId,
      List<DBFilterBase.IDFilter> idFilters) {
    List<Expression<BuiltCondition>> expressionsWithId = new ArrayList<>();

    if (idFilters.size() > 1) {
      throw new JsonApiException(
          ErrorCode.FILTER_MULTIPLE_ID_FILTER, ErrorCode.FILTER_MULTIPLE_ID_FILTER.getMessage());
    }
    if (idFilters.isEmpty()
        && additionalIdFilter == null) { // no idFilters in filter clause and no additionalIdFilter
      if (expressionWithoutId == null) {
        return null; // should find nothing
      } else {
        return List.of(expressionWithoutId);
      }
    }

    // have an idFilter
    DBFilterBase.IDFilter idFilter =
        additionalIdFilter != null ? additionalIdFilter : idFilters.get(0);
    if (idFilter.operator == DBFilterBase.IDFilter.Operator.IN && idFilter.getAll().isEmpty()) {
      return null; // should find nothing
    }
    // idFilter's operator is IN or EQ, for both, we can follow the split query logic
    List<BuiltCondition> inSplit =
        idFilters.isEmpty() ? new ArrayList<>() : idFilters.get(0).getAll();
    if (additionalIdFilter != null) {
      inSplit = additionalIdFilter.getAll(); // override the existed id filter
    }
    // split n queries by id
    return inSplit.stream()
        .map(
            idCondition -> {
              Expression<BuiltCondition> newExpression =
                  expressionWithoutId == null
                      ? Variable.of(idCondition)
                      : ExpressionUtils.andOf(Variable.of(idCondition), expressionWithoutId);
              return newExpression;
            })
        .collect(Collectors.toList());
  }

  private static Expression<BuiltCondition> buildExpressionRecursive(
      LogicalExpression logicalExpression,
      DBFilterBase.IDFilter additionalIdFilter,
      List<DBFilterBase.IDFilter> idConditionExpressions) {
    List<Expression<BuiltCondition>> conditionExpressions = new ArrayList<>();
    // first for loop, is to iterate all subLogicalExpression
    // each iteration goes into another recursive build
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      final Expression<BuiltCondition> subExpressionCondition =
          buildExpressionRecursive(
              subLogicalExpression, additionalIdFilter, idConditionExpressions);
      if (subExpressionCondition == null) {
        continue;
      }
      conditionExpressions.add(subExpressionCondition);
    }

    // if seeing $in or $nin, set hasInOrNinFilterThisLevel as true
    boolean hasInOrNinFilterThisLevel = false;
    boolean inFilterThisLevelWithEmptyArray = true;
    boolean ninFilterThisLevelWithEmptyArray = true;

    // second for loop, is to iterate all subComparisonExpression
    for (ComparisonExpression comparisonExpression : logicalExpression.comparisonExpressions) {
      for (DBFilterBase dbFilter : comparisonExpression.getDbFilters()) {
        if (dbFilter instanceof DBFilterBase.InFilter inFilter) {
          hasInOrNinFilterThisLevel = true;
          List<BuiltCondition> inFilterConditions = inFilter.getAll();
          if (!inFilterConditions.isEmpty()) {
            // store information of an empty array happens with $in or $nin
            if (inFilter.operator.equals(DBFilterBase.InFilter.Operator.IN)) {
              inFilterThisLevelWithEmptyArray = false;
            } else if (inFilter.operator.equals(DBFilterBase.InFilter.Operator.NIN)) {
              ninFilterThisLevelWithEmptyArray = false;
            }
            List<Variable<BuiltCondition>> inConditionsVariables =
                inFilterConditions.stream().map(Variable::of).toList();
            conditionExpressions.add(ExpressionUtils.orOf(inConditionsVariables));
          }
        } else if (dbFilter instanceof DBFilterBase.IDFilter idFilter) {
          if (additionalIdFilter == null) {
            idConditionExpressions.add(idFilter);
          }
        } else {
          conditionExpressions.add(Variable.of(dbFilter.get()));
        }
      }
    }

    // when having an empty array $nin, if $nin occurs within an $or logic, entire $or should match
    // everything
    if (hasInOrNinFilterThisLevel
        && ninFilterThisLevelWithEmptyArray
        && logicalExpression.getLogicalRelation().equals(LogicalExpression.LogicalOperator.OR)) {
      // TODO: find a better CQL TRUE placeholder
      conditionExpressions.clear();
      conditionExpressions.add(
          Variable.of(
              new DBFilterBase.IsNullFilter(
                      "something user never use", DBFilterBase.SetFilterBase.Operator.NOT_CONTAINS)
                  .get()));
      return ExpressionUtils.buildExpression(
          conditionExpressions, logicalExpression.getLogicalRelation().getOperator());
    }

    // when having an empty array $in, if $in occurs within an $and logic, entire $and should match
    // nothing
    if (hasInOrNinFilterThisLevel
        && inFilterThisLevelWithEmptyArray
        && logicalExpression.getLogicalRelation().equals(LogicalExpression.LogicalOperator.AND)) {
      return null;
    }

    // current logicalExpression is empty (implies sub-logicalExpression and
    // sub-comparisonExpression are all empty)
    if (conditionExpressions.isEmpty()) {
      return null;
    }

    return ExpressionUtils.buildExpression(
        conditionExpressions, logicalExpression.getLogicalRelation().getOperator());
  }

  /**
   * Get all positional cql values from express recursively. Result order is in consistent of the
   * expression structure
   */
  public static List<Object> getExpressionValuesInOrder(Expression<BuiltCondition> expression) {
    List<Object> values = new ArrayList<>();
    if (expression != null) {
      populateValuesRecursive(values, expression);
    }
    return values;
  }

  private static void populateValuesRecursive(
      List<Object> values, Expression<BuiltCondition> outerExpression) {
    if (outerExpression.getExprType().equals("variable")) {
      Variable<BuiltCondition> var = (Variable<BuiltCondition>) outerExpression;
      JsonTerm term = ((JsonTerm) var.getValue().value());
      if (term.getKey() != null) {
        values.add(term.getKey());
      }
      values.add(term.getValue());
      return;
    }
    if (outerExpression.getExprType().equals("and") || outerExpression.getExprType().equals("or")) {
      List<Expression<BuiltCondition>> innerExpressions = outerExpression.getChildren();
      for (Expression<BuiltCondition> innerExpression : innerExpressions) {
        populateValuesRecursive(values, innerExpression);
      }
    }
  }
}
