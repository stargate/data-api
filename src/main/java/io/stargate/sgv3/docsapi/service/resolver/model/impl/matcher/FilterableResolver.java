package io.stargate.sgv3.docsapi.service.resolver.model.impl.matcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.Filterable;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import io.stargate.sgv3.docsapi.service.operation.model.ReadOperation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.FindOperation;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
  private static final Object SINGLE_TEXT_GROUP = new Object();

  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();
  private static final Object DYNAMIC_BOOL_GROUP = new Object();
  private static final Object DYNAMIC_NULL_GROUP = new Object();

  private static final Object EMPTY_GROUP = new Object();
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
        .compareValues("_id", ValueComparisonOperator.EQ, JsonType.STRING);

    // NOTE - can only do eq ops on fields until SAI changes
    matchRules
        .addMatchRule(this::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", ValueComparisonOperator.EQ, JsonType.STRING)
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.NULL);
  }

  protected ReadOperation resolve(CommandContext commandContext, T command) {
    return matchRules.apply(commandContext, command);
  }

  public record FilteringOptions(int limit, String pagingState, int pageSize) {}

  protected abstract Optional<FilteringOptions> getFilteringOption(T command);

  private ReadOperation findById(CommandContext commandContext, CaptureGroups<T> captures) {
    List<FindOperation.DBFilterBase> filters = new ArrayList<>();

    final CaptureGroup<String> idGroup =
        (CaptureGroup<String>) captures.getGroupIfPresent(ID_GROUP);
    if (idGroup != null) {
      idGroup.consumeAllCaptures(
          expression ->
              filters.add(
                  new FindOperation.IDFilter(
                      FindOperation.IDFilter.Operator.EQ, expression.value())));
    }
    Optional<FilteringOptions> filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.isPresent()) {
      return new FindOperation(
          commandContext,
          filters,
          filteringOptions.get().pagingState(),
          filteringOptions.get().limit(),
          filteringOptions.get().pageSize(),
          readDocument,
          objectMapper);
    }
    throw new DocsException(ErrorCode.FILTER_UNRESOLVABLE);
  }

  private ReadOperation findNoFilter(CommandContext commandContext, CaptureGroups<T> captures) {
    Optional<FilteringOptions> filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.isPresent()) {
      return new FindOperation(
          commandContext,
          List.of(),
          filteringOptions.get().pagingState(),
          filteringOptions.get().limit(),
          filteringOptions.get().pageSize(),
          readDocument,
          objectMapper);
    }
    throw new DocsException(
        ErrorCode.FILTER_UNRESOLVABLE, "Options need to be returned for filterable of non findOne");
  }

  private ReadOperation findDynamic(CommandContext commandContext, CaptureGroups<T> captures) {
    List<FindOperation.DBFilterBase> filters = new ArrayList<>();

    final CaptureGroup<String> idGroup =
        (CaptureGroup<String>) captures.getGroupIfPresent(ID_GROUP);
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

    Optional<FilteringOptions> filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.isPresent()) {
      return new FindOperation(
          commandContext,
          filters,
          filteringOptions.get().pagingState(),
          filteringOptions.get().limit(),
          filteringOptions.get().pageSize(),
          readDocument,
          objectMapper);
    }
    throw new DocsException(
        ErrorCode.FILTER_UNRESOLVABLE, "Options need to be returned for filterable of non findOne");
  }
}
