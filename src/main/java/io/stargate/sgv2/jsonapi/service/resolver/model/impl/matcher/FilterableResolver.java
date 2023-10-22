package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ArrayComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ElementComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

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
    matchRules.addMatchRule(this::findNoFilter, FilterMatcher.MatchStrategy.EMPTY);

    matchRules
        .addMatchRule(this::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID);

    matchRules
        .addMatchRule(this::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP_IN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY);

    //    matchRules
    //            .addMatchRule(this::findDynamic, FilterMatcher.MatchStrategy.STRICT)
    //            .matcher()
    //            .capture(DYNAMIC_GROUP_IN)
    //            .compareValues("*", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY);

    // NOTE - can only do eq ops on fields until SAI changes
    matchRules
        .addMatchRule(this::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
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

  protected List<DBFilterBase> resolve(CommandContext commandContext, T command) {
    List<DBFilterBase> filter = matchRules.apply(commandContext, command);
    if (filter.size() > docLimits.maxFilterObjectProperties()) {
      throw new JsonApiException(
          ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION,
          String.format(
              "filter has %d fields, exceeds maximum allowed (%s)",
              ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION.getMessage(),
              filter.size(),
              docLimits.maxFilterObjectProperties()));
    }
    return matchRules.apply(commandContext, command);
  }

  private List<DBFilterBase> findById(CommandContext commandContext, CaptureGroups<T> captures) {
    List<DBFilterBase> filters = new ArrayList<>();
    final CaptureGroup<DocumentId> idGroup =
        (CaptureGroup<DocumentId>) captures.getGroupIfPresent(ID_GROUP);
    if (idGroup != null) {
      idGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, expression.value())));
    }

    final CaptureGroup<List<DocumentId>> idsGroup =
        (CaptureGroup<List<DocumentId>>) captures.getGroupIfPresent(ID_GROUP_IN);
    if (idsGroup != null) {
      idsGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.IN, expression.value())));
    }
    return filters;
  }

  private List<DBFilterBase> findNoFilter(
      CommandContext commandContext, CaptureGroups<T> captures) {
    return List.of();
  }

  private List<DBFilterBase> findDynamic(CommandContext commandContext, CaptureGroups<T> captures) {
    List<DBFilterBase> filters = new ArrayList<>();

    final CaptureGroup<DocumentId> idGroup =
        (CaptureGroup<DocumentId>) captures.getGroupIfPresent(ID_GROUP);
    if (idGroup != null) {
      idGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.EQ, List.of(expression.value()))));
    }

    final CaptureGroup<List<DocumentId>> idsGroup =
        (CaptureGroup<List<DocumentId>>) captures.getGroupIfPresent(ID_GROUP_IN);
    if (idsGroup != null) {
      idsGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.IDFilter(
                      DBFilterBase.IDFilter.Operator.IN, expression.value())));
    }

    final CaptureGroup<List<Object>> dynamicGroups =
        (CaptureGroup<List<Object>>) captures.getGroupIfPresent(DYNAMIC_GROUP_IN);
    if (dynamicGroups != null) {
      dynamicGroups.consumeAllCaptures(
          expression -> {
            final DocValueHasher docValueHasher = new DocValueHasher();
            filters.add(
                new DBFilterBase.InFilter(
                    DBFilterBase.InFilter.Operator.IN, expression.path(), expression.value()));
          });
    }

    final CaptureGroup<String> textGroup =
        (CaptureGroup<String>) captures.getGroupIfPresent(DYNAMIC_TEXT_GROUP);
    if (textGroup != null) {
      textGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.TextFilter(
                      expression.path(),
                      DBFilterBase.MapFilterBase.Operator.EQ,
                      expression.value())));
    }

    final CaptureGroup<Boolean> boolGroup =
        (CaptureGroup<Boolean>) captures.getGroupIfPresent(DYNAMIC_BOOL_GROUP);
    if (boolGroup != null) {
      boolGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.BoolFilter(
                      expression.path(),
                      DBFilterBase.MapFilterBase.Operator.EQ,
                      expression.value())));
    }

    final CaptureGroup<BigDecimal> numberGroup =
        (CaptureGroup<BigDecimal>) captures.getGroupIfPresent(DYNAMIC_NUMBER_GROUP);
    if (numberGroup != null) {
      numberGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.NumberFilter(
                      expression.path(),
                      DBFilterBase.MapFilterBase.Operator.EQ,
                      expression.value())));
    }

    final CaptureGroup<Object> nullGroup =
        (CaptureGroup<Object>) captures.getGroupIfPresent(DYNAMIC_NULL_GROUP);
    if (nullGroup != null) {
      nullGroup.consumeAllCaptures(
          expression -> filters.add(new DBFilterBase.IsNullFilter(expression.path())));
    }

    final CaptureGroup<Date> dateGroup =
        (CaptureGroup<Date>) captures.getGroupIfPresent(DYNAMIC_DATE_GROUP);
    if (dateGroup != null) {
      dateGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.DateFilter(
                      expression.path(),
                      DBFilterBase.MapFilterBase.Operator.EQ,
                      expression.value())));
    }

    final CaptureGroup<Boolean> existsGroup =
        (CaptureGroup<Boolean>) captures.getGroupIfPresent(EXISTS_GROUP);
    if (existsGroup != null) {
      existsGroup.consumeAllCaptures(
          expression -> {
            if (expression.value())
              filters.add(new DBFilterBase.ExistsFilter(expression.path(), expression.value()));
            else
              throw new JsonApiException(
                  ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
                  "$exists is supported only with true option");
          });
    }

    final CaptureGroup<List<Object>> allGroups =
        (CaptureGroup<List<Object>>) captures.getGroupIfPresent(ALL_GROUP);
    if (allGroups != null) {
      allGroups.consumeAllCaptures(
          expression -> {
            final DocValueHasher docValueHasher = new DocValueHasher();
            for (Object arrayValue : expression.value()) {
              filters.add(
                  new DBFilterBase.AllFilter(docValueHasher, expression.path(), arrayValue));
            }
          });
    }

    final CaptureGroup<BigDecimal> sizeGroups =
        (CaptureGroup<BigDecimal>) captures.getGroupIfPresent(SIZE_GROUP);
    if (sizeGroups != null) {
      sizeGroups.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.SizeFilter(expression.path(), expression.value().intValue())));
    }

    final CaptureGroup<List<Object>> arrayEqualsGroups =
        (CaptureGroup<List<Object>>) captures.getGroupIfPresent(ARRAY_EQUALS);
    if (arrayEqualsGroups != null) {
      arrayEqualsGroups.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.ArrayEqualsFilter(
                      new DocValueHasher(), expression.path(), expression.value())));
    }

    final CaptureGroup<Map<String, Object>> subDocEqualsGroups =
        (CaptureGroup<Map<String, Object>>) captures.getGroupIfPresent(SUB_DOC_EQUALS);
    if (subDocEqualsGroups != null) {
      subDocEqualsGroups.consumeAllCaptures(
          expression ->
              filters.add(
                  new DBFilterBase.SubDocEqualsFilter(
                      new DocValueHasher(), expression.path(), expression.value())));
    }
    return filters;
  }
}
