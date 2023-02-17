package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ArrayComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ElementComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.CountOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

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

  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();
  private static final Object DYNAMIC_BOOL_GROUP = new Object();
  private static final Object DYNAMIC_NULL_GROUP = new Object();
  private static final Object EXISTS_GROUP = new Object();
  private static final Object ALL_GROUP = new Object();
  private static final Object SIZE_GROUP = new Object();
  private static final Object ARRAY_EQUALS = new Object();
  private static final Object SUB_DOC_EQUALS = new Object();

  private final ObjectMapper objectMapper;

  protected FilterableResolver() {
    this(null);
  }

  @Inject
  public FilterableResolver(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    matchRules.addMatchRule(this::findNoFilter, FilterMatcher.MatchStrategy.EMPTY);

    matchRules
        .addMatchRule(this::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID);

    // NOTE - can only do eq ops on fields until SAI changes
    matchRules
        .addMatchRule(this::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID)
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL)
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

  protected ReadOperation resolve(CommandContext commandContext, T command) {
    return matchRules.apply(commandContext, command);
  }

  public record FilteringOptions(int limit, String pagingState, int pageSize, ReadType readType) {}

  protected abstract FilteringOptions getFilteringOption(T command);

  private ReadOperation findById(CommandContext commandContext, CaptureGroups<T> captures) {
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
    FilteringOptions filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.readType() == ReadType.COUNT) {
      return new CountOperation(commandContext, filters);
    } else {
      return new FindOperation(
          commandContext,
          filters,
          filteringOptions.pagingState(),
          filteringOptions.limit(),
          filteringOptions.pageSize(),
          filteringOptions.readType(),
          objectMapper);
    }
  }

  private ReadOperation findNoFilter(CommandContext commandContext, CaptureGroups<T> captures) {
    FilteringOptions filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.readType() == ReadType.COUNT) {
      return new CountOperation(commandContext, List.of());
    } else {
      return new FindOperation(
          commandContext,
          List.of(),
          filteringOptions.pagingState(),
          filteringOptions.limit(),
          filteringOptions.pageSize(),
          filteringOptions.readType(),
          objectMapper);
    }
  }

  private ReadOperation findDynamic(CommandContext commandContext, CaptureGroups<T> captures) {
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

    FilteringOptions filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.readType() == ReadType.COUNT) {
      return new CountOperation(commandContext, filters);
    } else {
      return new FindOperation(
          commandContext,
          filters,
          filteringOptions.pagingState(),
          filteringOptions.limit(),
          filteringOptions.pageSize(),
          filteringOptions.readType(),
          objectMapper);
    }
  }
}
