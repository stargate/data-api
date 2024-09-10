package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NumberTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.TextTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterLogicalExpression;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.function.BiConsumer;

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

  public static DBFilterLogicalExpression findNoFilter(
      DBFilterLogicalExpression dbFilterLogicalExpression, CaptureGroups currentCaptureGroups) {
    return dbFilterLogicalExpression;
  }

  public static DBFilterLogicalExpression findDynamic(
      DBFilterLogicalExpression currentDBFilterLogicalExpression,
      CaptureGroups currentCaptureGroups) {

    // TODO: How do we know what the CmdT of the JsonLiteral<CmdT> from .value() is ?
    BiConsumer<CaptureGroups, DBFilterLogicalExpression> consumer =
        (captureGroups, dbFilterLogicalExpression) -> {
          captureGroups
              .getGroupIfPresent(DYNAMIC_TEXT_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<String> dynamicTextGroup = (CaptureGroup<String>) captureGroup;
                    dynamicTextGroup.consumeAllCaptures(
                        expression -> {
                          dbFilterLogicalExpression.addInnerDBFilter(
                              new TextTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  (String) expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_NUMBER_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<BigDecimal> dynamicNumberGroup =
                        (CaptureGroup<BigDecimal>) captureGroup;
                    dynamicNumberGroup.consumeAllCaptures(
                        expression -> {
                          dbFilterLogicalExpression.addInnerDBFilter(
                              new NumberTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  (BigDecimal) expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_DOCID_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> dynamicDocIDGroup = (CaptureGroup<Object>) captureGroup;
                    dynamicDocIDGroup.consumeAllCaptures(
                        expression -> {
                          Object rhsValue = ((DocumentId) expression.value()).value();
                          if (rhsValue instanceof String) {
                            dbFilterLogicalExpression.addInnerDBFilter(
                                new TextTableFilter(
                                    expression.path(),
                                    NativeTypeTableFilter.Operator.from(
                                        (ValueComparisonOperator) expression.operator()),
                                    (String) rhsValue));
                          } else if (rhsValue instanceof Number) {
                            dbFilterLogicalExpression.addInnerDBFilter(
                                new NumberTableFilter(
                                    expression.path(),
                                    NativeTypeTableFilter.Operator.from(
                                        (ValueComparisonOperator) expression.operator()),
                                    (Number) rhsValue));
                          } else {
                            throw new UnsupportedOperationException(
                                "Unsupported DocumentId type: " + rhsValue.getClass().getName());
                          }
                        });
                  });
        };

    currentCaptureGroups.recursiveConsume(currentDBFilterLogicalExpression, consumer);
    return currentDBFilterLogicalExpression;
  }
}
