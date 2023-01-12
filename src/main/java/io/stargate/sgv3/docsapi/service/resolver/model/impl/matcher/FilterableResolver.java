package io.stargate.sgv3.docsapi.service.resolver.model.impl.matcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.Filterable;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv3.docsapi.api.model.command.clause.filter.ValueComparisonOperator;
import io.stargate.sgv3.docsapi.exception.DocsException;
import io.stargate.sgv3.docsapi.exception.ErrorCode;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv3.docsapi.service.resolver.model.CommandResolver;
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
public abstract class FilterableResolver<T extends Command & Filterable>
    implements CommandResolver<T> {

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

  @Inject ObjectMapper objectMapper;

  public FilterableResolver(boolean findOne, boolean readDocument) {
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
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.NULL);
  }

  @Override
  public abstract Class<T> getCommandClass();

  @Override
  public Operation resolveCommand(CommandContext commandContext, T command) {
    return matchRules.apply(commandContext, command);
  }

  protected static final class FilteringOptions {
    private final int limit;
    private final String pagingState;

    public FilteringOptions() {
      this(0, null);
    }

    public FilteringOptions(int limit, String pagingState) {
      this.limit = limit;
      this.pagingState = pagingState;
    }

    public int getLimit() {
      return limit;
    }

    public String getPagingState() {
      return pagingState;
    }
  }

  protected abstract Optional<FilteringOptions> getFilteringOption(T command);

  private Operation findById(CommandContext commandContext, CaptureGroups<T> captures) {
    List<FindOperation.DBFilterBase> filters = new ArrayList<>();

    final CaptureGroup<String> idGroup =
        (CaptureGroup<String>) captures.getGroupIfPresent(ID_GROUP);
    if (idGroup != null) {
      idGroup.consumeAllCaptures(
          pair ->
              filters.add(
                  new FindOperation.IDFilter(FindOperation.IDFilter.Operator.EQ, pair.value())));
    }
    Optional<FilteringOptions> filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.isPresent()) {
      return new FindOperation(
          commandContext,
          filters,
          filteringOptions.get().pagingState,
          filteringOptions.get().limit,
          readDocument,
          objectMapper);
    }
    throw new DocsException(ErrorCode.FILTER_UNRESOLVABLE);
  }

  private Operation findNoFilter(CommandContext commandContext, CaptureGroups<T> captures) {
    Optional<FilteringOptions> filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.isPresent()) {
      return new FindOperation(
          commandContext,
          List.of(),
          filteringOptions.get().pagingState,
          filteringOptions.get().limit,
          readDocument,
          objectMapper);
    }
    throw new DocsException(
        ErrorCode.FILTER_UNRESOLVABLE, "Options need to be returned for filterable of non findOne");
  }

  private Operation findDynamic(CommandContext commandContext, CaptureGroups<T> captures) {
    List<FindOperation.DBFilterBase> filters = new ArrayList<>();

    final CaptureGroup<String> textGroup =
        (CaptureGroup<String>) captures.getGroupIfPresent(DYNAMIC_TEXT_GROUP);
    if (textGroup != null) {
      textGroup.consumeAllCaptures(
          pair ->
              filters.add(
                  new FindOperation.TextFilter(
                      pair.path(), FindOperation.MapFilterBase.Operator.EQ, pair.value())));
    }

    final CaptureGroup<Boolean> boolGroup =
        (CaptureGroup<Boolean>) captures.getGroupIfPresent(DYNAMIC_BOOL_GROUP);
    if (boolGroup != null) {
      boolGroup.consumeAllCaptures(
          pair ->
              filters.add(
                  new FindOperation.BoolFilter(
                      pair.path(), FindOperation.MapFilterBase.Operator.EQ, pair.value())));
    }

    final CaptureGroup<BigDecimal> numberGroup =
        (CaptureGroup<BigDecimal>) captures.getGroupIfPresent(DYNAMIC_NUMBER_GROUP);
    if (numberGroup != null) {
      numberGroup.consumeAllCaptures(
          pair ->
              filters.add(
                  new FindOperation.NumberFilter(
                      pair.path(), FindOperation.MapFilterBase.Operator.EQ, pair.value())));
    }

    final CaptureGroup<Object> nullGroup =
        (CaptureGroup<Object>) captures.getGroupIfPresent(DYNAMIC_NULL_GROUP);
    if (nullGroup != null) {
      nullGroup.consumeAllCaptures(
          pair -> filters.add(new FindOperation.IsNullFilter(pair.path())));
    }

    Optional<FilteringOptions> filteringOptions = getFilteringOption(captures.command());
    if (filteringOptions.isPresent()) {
      return new FindOperation(
          commandContext,
          filters,
          filteringOptions.get().pagingState,
          filteringOptions.get().limit,
          readDocument,
          objectMapper);
    }
    throw new DocsException(
        ErrorCode.FILTER_UNRESOLVABLE, "Options need to be returned for filterable of non findOne");
  }
}
