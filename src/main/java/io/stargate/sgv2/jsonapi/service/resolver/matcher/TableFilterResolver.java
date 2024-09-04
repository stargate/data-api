package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NumberTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.TextTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * POC for a filter clause resolver that can handle the filter clause for a table.
 *
 * @param <CmdT>
 */
public class TableFilterResolver<CmdT extends Command & Filterable>
    extends FilterResolver<CmdT, TableSchemaObject> {

  private static final Object DYNAMIC_DOCID_GROUP = new Object();
  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();

  public TableFilterResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);
  }

  @Override
  protected FilterMatchRules<CmdT> buildMatchRules() {
    var matchRules = new FilterMatchRules<CmdT>();

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
            JsonType.NUMBER)
        // Although Tables does not have special handling for _id, our FilterClauseDeserializer
        // does, so we need to capture it here.
        .capture(DYNAMIC_DOCID_GROUP)
        .compareValues(
            "_id",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                //                ValueComparisonOperator.NE, // TODO: not sure this is supported
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.DOCUMENT_ID);

    return matchRules;
  }

  private static List<DBFilterBase> findNoFilter(CaptureExpression captureExpression) {
    return List.of();
  }

  private static List<DBFilterBase> findDynamic(CaptureExpression captureExpression) {
    List<DBFilterBase> filters = new ArrayList<>();

    // TODO: How do we know what the CmdT of the JsonLiteral<CmdT> from .value() is ?
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {
      final Object rhsValue = filterOperation.operand().value();
      if (captureExpression.marker() == DYNAMIC_TEXT_GROUP) {
        filters.add(
            new TextTableFilter(
                captureExpression.path(),
                NativeTypeTableFilter.Operator.from(
                    (ValueComparisonOperator) filterOperation.operator()),
                (String) rhsValue));
      } else if (captureExpression.marker() == DYNAMIC_NUMBER_GROUP) {
        filters.add(
            new NumberTableFilter(
                captureExpression.path(),
                NativeTypeTableFilter.Operator.from(
                    (ValueComparisonOperator) filterOperation.operator()),
                (Number) rhsValue));
      } else if (captureExpression.marker() == DYNAMIC_DOCID_GROUP) {
        Object actualValue = ((DocumentId) rhsValue).value();
        if (actualValue instanceof String) {
          filters.add(
              new TextTableFilter(
                  captureExpression.path(),
                  NativeTypeTableFilter.Operator.from(
                      (ValueComparisonOperator) filterOperation.operator()),
                  (String) actualValue));
        } else if (actualValue instanceof Number) {
          filters.add(
              new NumberTableFilter(
                  captureExpression.path(),
                  NativeTypeTableFilter.Operator.from(
                      (ValueComparisonOperator) filterOperation.operator()),
                  (Number) actualValue));
        } else {
          throw new UnsupportedOperationException(
              "Unsupported DocumentId type: " + rhsValue.getClass().getName());
        }
      } else {
        throw new UnsupportedOperationException(
            "Unsupported dynamic filter type: " + filterOperation);
      }
    }

    return filters;
  }
}
