package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NumberTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.TextTableFilter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class TableFilterResolver<T extends Command & Filterable>
    extends FilterResolver<T, TableSchemaObject> {

  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();

  public TableFilterResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  @Override
  protected FilterMatchRules<T> buildMatchRules() {
    var matchRules = new FilterMatchRules<T>();

    matchRules.addMatchRule(TableFilterResolver::findNoFilter, FilterMatcher.MatchStrategy.EMPTY);

    matchRules
        .addMatchRule(TableFilterResolver::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
        .matcher()
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                //                ValueComparisonOperator.NE, // TODO: not sure this is supported
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.STRING)
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                //                ValueComparisonOperator.NE, - TODO - not supported
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.NUMBER);

    return matchRules;
  }

  private static List<DBFilterBase> findNoFilter(CaptureExpression captureExpression) {
    return List.of();
  }

  private static List<DBFilterBase> findDynamic(CaptureExpression captureExpression) {
    List<DBFilterBase> filters = new ArrayList<>();

    // TODO: How do we know what the T of the JsonLiteral<T> from .value() is ?
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {
      if (captureExpression.marker() == DYNAMIC_TEXT_GROUP) {
        filters.add(
            new TextTableFilter(
                captureExpression.path(),
                NativeTypeTableFilter.Operator.from(
                    (ValueComparisonOperator) filterOperation.operator()),
                (String) filterOperation.operand().value()));
      } else if (captureExpression.marker() == DYNAMIC_NUMBER_GROUP) {
        filters.add(
            new NumberTableFilter(
                captureExpression.path(),
                NativeTypeTableFilter.Operator.from(
                    (ValueComparisonOperator) filterOperation.operator()),
                (BigDecimal) filterOperation.operand().value()));
      }
    }

    return filters;
  }
}