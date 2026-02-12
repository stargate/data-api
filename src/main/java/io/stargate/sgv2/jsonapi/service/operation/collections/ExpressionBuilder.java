package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Variable;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.service.cql.ExpressionUtils;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.*;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ExpressionBuilder {

  public static List<Expression<BuiltCondition>> buildExpressions(
      DBLogicalExpression dbLogicalExpression, IDCollectionFilter additionalIdFilter) {
    // an empty filter should find everything
    if (dbLogicalExpression.isEmpty() && additionalIdFilter == null) {
      return Collections.singletonList(null);
    }
    // after validate in FilterClauseDeserializer,
    // partition key column key will not be nested under OR operator
    // so we can collect all id_conditions, then do a combination to generate separate queries
    List<IDCollectionFilter> idFilters = new ArrayList<>();
    // expressionWithoutId must be a And(if not null)
    // since we have outer implicit and in the filter
    Expression<BuiltCondition> expressionWithoutId =
        buildExpressionRecursive(dbLogicalExpression, additionalIdFilter, idFilters);
    return buildExpressionWithId(additionalIdFilter, expressionWithoutId, idFilters);
  }

  // buildExpressionWithId only handles IDFilter ($eq, $ne, $in)
  private static List<Expression<BuiltCondition>> buildExpressionWithId(
      IDCollectionFilter additionalIdFilter,
      Expression<BuiltCondition> expressionWithoutId,
      List<IDCollectionFilter> idFilters) {
    if (idFilters.size() > 1) {
      throw FilterException.Code.FILTER_MULTIPLE_ID_FILTER.get();
    }
    if (idFilters.isEmpty()
        && additionalIdFilter == null) { // no idFilters in filter clause and no additionalIdFilter
      if (expressionWithoutId == null) {
        // no valid non_id filters (eg. "name":{"$nin" : []} ) and no id filter
        return Collections.singletonList(null); // should find everything
      }
      return List.of(expressionWithoutId);
    }

    // have an idFilter
    IDCollectionFilter idFilter =
        additionalIdFilter != null ? additionalIdFilter : idFilters.getFirst();

    // _id: {$in: []} should find nothing in the entire query
    // since _id can not work with $or, entire $and should find nothing
    if (idFilter.operator == IDCollectionFilter.Operator.IN && idFilter.getAll().isEmpty()) {
      return null; // should find nothing
    }

    // idFilter's operator is IN/EQ/NE, for both, split into n query logic
    List<BuiltCondition> inSplit =
        idFilters.isEmpty() ? new ArrayList<>() : idFilters.getFirst().getAll();
    if (additionalIdFilter != null) {
      inSplit = additionalIdFilter.getAll(); // override the existed id filter
    }
    return inSplit.stream()
        .map(
            idCondition ->
                (expressionWithoutId == null)
                    ? Variable.of(idCondition)
                    : ExpressionUtils.andOf(Variable.of(idCondition), expressionWithoutId))
        .collect(Collectors.toList());
  }

  private static Expression<BuiltCondition> buildExpressionRecursive(
      DBLogicalExpression dbLogicalExpression,
      IDCollectionFilter additionalIdFilter,
      List<IDCollectionFilter> idConditionExpressions) {
    List<Expression<BuiltCondition>> conditionExpressions = new ArrayList<>();
    // first for loop, is to iterate all subLogicalExpression
    // each iteration goes into another recursive build
    for (DBLogicalExpression subLogicalExpression : dbLogicalExpression.subExpressions()) {
      final Expression<BuiltCondition> subExpressionCondition =
          buildExpressionRecursive(
              subLogicalExpression, additionalIdFilter, idConditionExpressions);
      if (subExpressionCondition == null) {
        continue;
      }
      conditionExpressions.add(subExpressionCondition);
    }

    // if seeing $in, set hasInFilterThisLevel as true
    boolean hasInFilterThisLevel = false;
    // if seeing $nin, set hasNinFilterThisLevel as true
    boolean hasNinFilterThisLevel = false;
    boolean inFilterThisLevelWithEmptyArray = true;
    boolean ninFilterThisLevelWithEmptyArray = true;

    // second for loop, is to iterate dbFilters
    for (DBFilterBase dbFilter : dbLogicalExpression.filters()) {
      if (dbFilter instanceof AllCollectionFilter allFilter) {
        List<BuiltCondition> allFilterConditions = allFilter.getAll();
        List<Variable<BuiltCondition>> allFilterVariables =
            allFilterConditions.stream().map(Variable::of).toList();
        conditionExpressions.add(
            allFilter.isNegation()
                ? ExpressionUtils.orOf(allFilterVariables)
                : ExpressionUtils.andOf(allFilterVariables));
      } else if (dbFilter instanceof InCollectionFilter inFilter) {
        if (inFilter.operator.equals(InCollectionFilter.Operator.IN)) {
          hasInFilterThisLevel = true;
        } else if (inFilter.operator.equals(InCollectionFilter.Operator.NIN)) {
          hasNinFilterThisLevel = true;
        }
        List<BuiltCondition> inFilterConditions = inFilter.getAll();
        if (!inFilterConditions.isEmpty()) {
          // store information of an empty array happens with $in or $nin
          if (inFilter.operator.equals(InCollectionFilter.Operator.IN)) {
            inFilterThisLevelWithEmptyArray = false;
          } else if (inFilter.operator.equals(InCollectionFilter.Operator.NIN)) {
            ninFilterThisLevelWithEmptyArray = false;
          }
          List<Variable<BuiltCondition>> inConditionsVariables =
              inFilterConditions.stream().map(Variable::of).toList();
          // non_id $in:["A","B"] -> array_contains contains A or array_contains contains B
          // non_id $nin:["A","B"] -> array_contains not contains A and array_contains not
          // contains B
          // _id $nin: ["A","B"] -> query_text_values['_id'] != A and query_text_values['_id'] !=
          // B
          conditionExpressions.add(
              inFilter.operator.equals(InCollectionFilter.Operator.IN)
                  ? ExpressionUtils.orOf(inConditionsVariables)
                  : ExpressionUtils.andOf(inConditionsVariables));
        }
      } else if (dbFilter instanceof IDCollectionFilter idFilter) {
        if (additionalIdFilter == null) {
          idConditionExpressions.add(idFilter);
        }
      } else {
        conditionExpressions.add(Variable.of(dbFilter.get()));
      }
    }

    // when having an empty array $nin, if $nin occurs within an or logic, entire or should match
    // everything
    if (hasNinFilterThisLevel
        && ninFilterThisLevelWithEmptyArray
        && dbLogicalExpression.operator().equals(DBLogicalExpression.DBLogicalOperator.OR)) {
      // TODO: find a better CQL TRUE placeholder
      conditionExpressions.clear();
      conditionExpressions.add(
          Variable.of(
              new IsNullCollectionFilter(
                      "something user never use", SetCollectionFilter.Operator.NOT_CONTAINS)
                  .get()));
      return ExpressionUtils.buildExpression(conditionExpressions, dbLogicalExpression.operator());
    }

    // when having an empty array $in, if $in occurs within an and logic, entire and should match
    // nothing
    if (hasInFilterThisLevel
        && inFilterThisLevelWithEmptyArray
        && dbLogicalExpression.operator().equals(DBLogicalExpression.DBLogicalOperator.AND)) {
      // TODO: find a better CQL FALSE placeholder
      conditionExpressions.clear();
      conditionExpressions.add(
          Variable.of(
              new IsNullCollectionFilter(
                      "something user never use", SetCollectionFilter.Operator.CONTAINS)
                  .get()));
      return ExpressionUtils.buildExpression(conditionExpressions, dbLogicalExpression.operator());
    }

    // current dbLogicalExpression is empty (implies nested dbLogicalExpression and dbFilters are
    // all empty)
    if (conditionExpressions.isEmpty()) {
      return null;
    }

    return ExpressionUtils.buildExpression(conditionExpressions, dbLogicalExpression.operator());
  }
}
