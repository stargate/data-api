package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.*;

/**
 * Base for resolvers that are {@link Filterable}, there are a number of commands like find,
 * findOne, updateOne that all have a filter.
 *
 * <p>There will be some re-use, and some customisation to work out.
 *
 * <p>T - type of the command we are resolving
 */
public abstract class FilterableResolver<T extends Command & Filterable> {

  private final FilterMatchRules<T> matchRules = new FilterMatchRules<>();

  private static final Object ID_GROUP = new Object();
  private static final Object ID_GROUP_IN = new Object();
  private static final Object DYNAMIC_GROUP_IN = new Object();
  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();
  private static final Object DYNAMIC_BOOL_GROUP = new Object();
  private static final Object DYNAMIC_NULL_GROUP = new Object();
  private static final Object DYNAMIC_DATE_GROUP = new Object();
  private static final Object EXISTS_GROUP = new Object();
  private static final Object ALL_GROUP = new Object();
  private static final Object SIZE_GROUP = new Object();
  private static final Object ARRAY_EQUALS = new Object();
  private static final Object SUB_DOC_EQUALS = new Object();
  @Inject DocumentLimitsConfig docLimits;

  @Inject
  public FilterableResolver() {
    matchRules.addMatchRule(FilterableResolver::findNoFilter, FilterMatcher.MatchStrategy.EMPTY);
    matchRules
        .addMatchRule(FilterableResolver::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID);

    matchRules
        .addMatchRule(FilterableResolver::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP_IN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY);

    //     NOTE - can only do eq ops on fields until SAI changes
    matchRules
        .addMatchRule(FilterableResolver::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID)
        .capture(ID_GROUP_IN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY)
        .capture(DYNAMIC_GROUP_IN)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY)
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL)
        .capture(DYNAMIC_DATE_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DATE)
        .capture(EXISTS_GROUP)
        .compareValues("*", EnumSet.of(ElementComparisonOperator.EXISTS), JsonType.BOOLEAN)
        .capture(ALL_GROUP)
        .compareValues("*", EnumSet.of(ArrayComparisonOperator.ALL), JsonType.ARRAY)
        .capture(SIZE_GROUP)
        .compareValues("*", EnumSet.of(ArrayComparisonOperator.SIZE), JsonType.NUMBER)
        .capture(ARRAY_EQUALS)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.ARRAY)
        .capture(SUB_DOC_EQUALS)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.SUB_DOC);
  }

  protected LogicalExpression resolve(CommandContext commandContext, T command) {
    LogicalExpression filter = matchRules.apply(commandContext, command);
    if (filter.getTotalComparisonExpressionCount() > docLimits.maxFilterObjectProperties()) {
      throw new JsonApiException(
          ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION,
          String.format(
              "%s: filter has %d fields, exceeds maximum allowed %s",
              ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION.getMessage(),
              filter.getTotalComparisonExpressionCount(),
              docLimits.maxFilterObjectProperties()));
    }
    return filter;
  }

  public static List<DBFilterBase> findById(CaptureExpression captureExpression) {

    List<DBFilterBase> filters = new ArrayList<>();
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {
      if (captureExpression.marker() == ID_GROUP) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.EQ, (DocumentId) filterOperation.operand().value()));
      }
      if (captureExpression.marker() == ID_GROUP_IN) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.IN,
                (List<DocumentId>) filterOperation.operand().value()));
      }
    }
    return filters;
  }

  public static List<DBFilterBase> findNoFilter(CaptureExpression captureExpression) {
    return List.of();
  }

  public static List<DBFilterBase> findDynamic(CaptureExpression captureExpression) {
    List<DBFilterBase> filters = new ArrayList<>();
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {

      if (captureExpression.marker() == ID_GROUP) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.EQ, (DocumentId) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == ID_GROUP_IN) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.IN,
                (List<DocumentId>) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_GROUP_IN) {
        filters.add(
            new DBFilterBase.InFilter(
                DBFilterBase.InFilter.Operator.IN,
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_TEXT_GROUP) {
        filters.add(
            new DBFilterBase.TextFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.EQ,
                (String) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_BOOL_GROUP) {
        filters.add(
            new DBFilterBase.BoolFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.EQ,
                (Boolean) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NUMBER_GROUP) {
        filters.add(
            new DBFilterBase.NumberFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.EQ,
                (BigDecimal) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NULL_GROUP) {
        filters.add(new DBFilterBase.IsNullFilter(captureExpression.path()));
      }

      if (captureExpression.marker() == DYNAMIC_DATE_GROUP) {
        filters.add(
            new DBFilterBase.DateFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.EQ,
                (Date) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == EXISTS_GROUP) {
        Boolean bool = (Boolean) filterOperation.operand().value();
        filters.add(new DBFilterBase.ExistsFilter(captureExpression.path(), bool));
      }

      if (captureExpression.marker() == ALL_GROUP) {
        final DocValueHasher docValueHasher = new DocValueHasher();
        List<Object> objects = (List<Object>) filterOperation.operand().value();
        for (Object arrayValue : objects) {
          filters.add(
              new DBFilterBase.AllFilter(docValueHasher, captureExpression.path(), arrayValue));
        }
      }

      if (captureExpression.marker() == SIZE_GROUP) {
        BigDecimal bigDecimal = (BigDecimal) filterOperation.operand().value();
        filters.add(new DBFilterBase.SizeFilter(captureExpression.path(), bigDecimal.intValue()));
      }

      if (captureExpression.marker() == ARRAY_EQUALS) {
        filters.add(
            new DBFilterBase.ArrayEqualsFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == SUB_DOC_EQUALS) {
        filters.add(
            new DBFilterBase.SubDocEqualsFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (Map<String, Object>) filterOperation.operand().value()));
      }
    }

    return filters;
  }
}
