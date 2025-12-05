package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ArrayComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.*;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.BinaryTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
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
  private static final Object DYNAMIC_BOOL_GROUP = new Object();
  private static final Object DYNAMIC_EJSON_GROUP = new Object();
  private static final Object DYNAMIC_GROUP_IN = new Object();
  private static final Object DYNAMIC_MAP_SET_LIST_GROUP = new Object();
  private static final Object DYNAMIC_MATCH_GROUP = new Object();

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
                ValueComparisonOperator.NE,
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
                ValueComparisonOperator.NE,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.NUMBER)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                ValueComparisonOperator.NE,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.BOOLEAN)
        // For now only EJSON support is for binary data
        .capture(DYNAMIC_EJSON_GROUP)
        .compareValues(
            "*",
            EnumSet.of(ValueComparisonOperator.EQ, ValueComparisonOperator.NE),
            JsonType.EJSON_WRAPPER)
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
            JsonType.DOCUMENT_ID)
        .capture(DYNAMIC_GROUP_IN)
        .compareValues(
            "*",
            EnumSet.of(ValueComparisonOperator.IN, ValueComparisonOperator.NIN),
            JsonType.ARRAY)
        // Four filter operators are allowed for map/set/list, value type must be ARRAY.
        .capture(DYNAMIC_MAP_SET_LIST_GROUP)
        .compareMapSetListFilterValues(
            "*",
            Set.of(
                ValueComparisonOperator.IN,
                ValueComparisonOperator.NIN,
                ArrayComparisonOperator.ALL,
                // NOTANY is not public exposed in API, only be negated by ALL.
                ArrayComparisonOperator.NOTANY),
            JsonType.ARRAY)
        .capture(DYNAMIC_MATCH_GROUP) // For $match operator
        .compareValues("*", EnumSet.of(ValueComparisonOperator.MATCH), JsonType.STRING);
    return matchRules;
  }

  public static DBLogicalExpression findNoFilter(
      DBLogicalExpression dbLogicalExpression, CaptureGroups currentCaptureGroups) {
    return dbLogicalExpression;
  }

  public static DBLogicalExpression findDynamic(
      DBLogicalExpression currentDBLogicalExpression, CaptureGroups currentCaptureGroups) {

    // TODO: How do we know what the CmdT of the JsonLiteral<CmdT> from .value() is ?
    BiConsumer<CaptureGroups, DBLogicalExpression> consumer =
        (captureGroups, dbLogicalExpression) -> {
          captureGroups
              .getGroupIfPresent(DYNAMIC_TEXT_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<String> dynamicTextGroup = (CaptureGroup<String>) captureGroup;
                    dynamicTextGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new TextTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  expression.value()));
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
                          dbLogicalExpression.addFilter(
                              new NumberTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  (BigDecimal) expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_BOOL_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Boolean> dynamicNumberGroup = (CaptureGroup<Boolean>) captureGroup;
                    dynamicNumberGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new BooleanTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  (Boolean) expression.value()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_EJSON_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> dynamicNumberGroup = (CaptureGroup<Object>) captureGroup;
                    dynamicNumberGroup.consumeAllCaptures(
                        expression -> {
                          byte[] bytes = (byte[]) expression.value();
                          EJSONWrapper boundValue = EJSONWrapper.binaryWrapper(bytes);
                          dbLogicalExpression.addFilter(
                              new BinaryTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  boundValue));
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
                            dbLogicalExpression.addFilter(
                                new TextTableFilter(
                                    expression.path(),
                                    NativeTypeTableFilter.Operator.from(
                                        (ValueComparisonOperator) expression.operator()),
                                    (String) rhsValue));
                          } else if (rhsValue instanceof Number) {
                            dbLogicalExpression.addFilter(
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

          captureGroups
              .getGroupIfPresent(DYNAMIC_GROUP_IN)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> dynamicInGroup = (CaptureGroup<Object>) captureGroup;
                    dynamicInGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new InTableFilter(
                                  InTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  expression.path(),
                                  (List<Object>) expression.value()));
                        });
                  });

          // consume captures for map/set/list and convert to MapSetListTableFilter
          captureGroups
              .getGroupIfPresent(DYNAMIC_MAP_SET_LIST_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<Object> mapSetListGroup = (CaptureGroup<Object>) captureGroup;
                    mapSetListGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new MapSetListTableFilter(
                                  MapSetListTableFilter.Operator.from(expression.operator()),
                                  expression.path(),
                                  (List<Object>) expression.value(),
                                  expression.filterComponent()));
                        });
                  });

          captureGroups
              .getGroupIfPresent(DYNAMIC_MATCH_GROUP)
              .ifPresent(
                  captureGroup -> {
                    CaptureGroup<String> dynamicMatchGroup = (CaptureGroup<String>) captureGroup;
                    dynamicMatchGroup.consumeAllCaptures(
                        expression -> {
                          dbLogicalExpression.addFilter(
                              new TextTableFilter(
                                  expression.path(),
                                  NativeTypeTableFilter.Operator.from(
                                      (ValueComparisonOperator) expression.operator()),
                                  expression.value()));
                        });
                  });
        };

    currentCaptureGroups.consumeAll(currentDBLogicalExpression, consumer);
    return currentDBLogicalExpression;
  }
}
