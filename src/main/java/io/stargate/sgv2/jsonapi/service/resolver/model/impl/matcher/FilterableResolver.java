package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ElementComparisonOperator;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
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

  private final boolean findOne;
  private final boolean readDocument;

  private final ObjectMapper objectMapper;

  protected FilterableResolver() {
    this(null, false, false);
  }

  @Inject
  public FilterableResolver(ObjectMapper objectMapper, boolean findOne, boolean readDocument) {
    this.objectMapper = objectMapper;
    this.findOne = findOne;
    this.readDocument = readDocument;
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
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING)
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL)
        .capture(EXISTS_GROUP)
        .compareValues("*", EnumSet.of(ElementComparisonOperator.EXISTS), JsonType.BOOLEAN);
    ;
  }

  protected ReadOperation resolve(CommandContext commandContext, T command) {
    return matchRules.apply(commandContext, command);
  }

  public record FilteringOptions(int limit, String pagingState, int pageSize) {}

  protected abstract FilteringOptions getFilteringOption(T command);

  private ReadOperation findById(CommandContext commandContext, CaptureGroups<T> captures) {
    List<FindOperation.DBFilterBase> filters = new ArrayList<>();

    final CaptureGroup<DocumentId> idGroup =
        (CaptureGroup<DocumentId>) captures.getGroupIfPresent(ID_GROUP);
    if (idGroup != null) {
      idGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new FindOperation.IDFilter(
                      FindOperation.IDFilter.Operator.EQ, expression.value())));
    }
    FilteringOptions filteringOptions = getFilteringOption(captures.command());
    return new FindOperation(
        commandContext,
        filters,
        filteringOptions.pagingState(),
        filteringOptions.limit(),
        filteringOptions.pageSize(),
        readDocument,
        objectMapper);
  }

  private ReadOperation findNoFilter(CommandContext commandContext, CaptureGroups<T> captures) {
    FilteringOptions filteringOptions = getFilteringOption(captures.command());
    return new FindOperation(
        commandContext,
        List.of(),
        filteringOptions.pagingState(),
        filteringOptions.limit(),
        filteringOptions.pageSize(),
        readDocument,
        objectMapper);
  }

  private ReadOperation findDynamic(CommandContext commandContext, CaptureGroups<T> captures) {
    List<FindOperation.DBFilterBase> filters = new ArrayList<>();

    final CaptureGroup<DocumentId> idGroup =
        (CaptureGroup<DocumentId>) captures.getGroupIfPresent(ID_GROUP);
    if (idGroup != null) {
      idGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new FindOperation.IDFilter(
                      FindOperation.IDFilter.Operator.EQ, expression.value())));
    }

    final CaptureGroup<String> textGroup =
        (CaptureGroup<String>) captures.getGroupIfPresent(DYNAMIC_TEXT_GROUP);
    if (textGroup != null) {
      textGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new FindOperation.TextFilter(
                      expression.path(),
                      FindOperation.MapFilterBase.Operator.EQ,
                      expression.value())));
    }

    final CaptureGroup<Boolean> boolGroup =
        (CaptureGroup<Boolean>) captures.getGroupIfPresent(DYNAMIC_BOOL_GROUP);
    if (boolGroup != null) {
      boolGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new FindOperation.BoolFilter(
                      expression.path(),
                      FindOperation.MapFilterBase.Operator.EQ,
                      expression.value())));
    }

    final CaptureGroup<BigDecimal> numberGroup =
        (CaptureGroup<BigDecimal>) captures.getGroupIfPresent(DYNAMIC_NUMBER_GROUP);
    if (numberGroup != null) {
      numberGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new FindOperation.NumberFilter(
                      expression.path(),
                      FindOperation.MapFilterBase.Operator.EQ,
                      expression.value())));
    }

    final CaptureGroup<Object> nullGroup =
        (CaptureGroup<Object>) captures.getGroupIfPresent(DYNAMIC_NULL_GROUP);
    if (nullGroup != null) {
      nullGroup.consumeAllCaptures(
          expression -> filters.add(new FindOperation.IsNullFilter(expression.path())));
    }

    final CaptureGroup<Boolean> existsGroup =
        (CaptureGroup<Boolean>) captures.getGroupIfPresent(EXISTS_GROUP);
    if (existsGroup != null) {
      existsGroup.consumeAllCaptures(
          expression -> {
            if (expression.value())
              filters.add(new FindOperation.ExistsFilter(expression.path(), expression.value()));
            else
              throw new JsonApiException(
                  ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
                  "$exists is supported only with true option");
          });
    }

    FilteringOptions filteringOptions = getFilteringOption(captures.command());
    return new FindOperation(
        commandContext,
        filters,
        filteringOptions.pagingState(),
        filteringOptions.limit(),
        filteringOptions.pageSize(),
        readDocument,
        objectMapper);
  }
}
