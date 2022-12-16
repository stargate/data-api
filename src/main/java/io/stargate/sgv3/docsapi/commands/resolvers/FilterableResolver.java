package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause.Filterable;
import io.stargate.sgv3.docsapi.commands.clauses.filter.ValueComparisonOperator;
import io.stargate.sgv3.docsapi.commands.resolvers.CaptureGroup.CapturePairLiteral;
import io.stargate.sgv3.docsapi.commands.resolvers.FilterMatcher.MatchStrategy;
import io.stargate.sgv3.docsapi.operations.FindByIdOperation;
import io.stargate.sgv3.docsapi.operations.FindDynamicQueryOperation;
import io.stargate.sgv3.docsapi.operations.FindDynamicQueryOperation.BoolFilter;
import io.stargate.sgv3.docsapi.operations.FindDynamicQueryOperation.DBFilterBase;
import io.stargate.sgv3.docsapi.operations.FindDynamicQueryOperation.IsNullFilter;
import io.stargate.sgv3.docsapi.operations.FindDynamicQueryOperation.MapFilterBase;
import io.stargate.sgv3.docsapi.operations.FindDynamicQueryOperation.NumberFilter;
import io.stargate.sgv3.docsapi.operations.FindDynamicQueryOperation.TextFilter;
import io.stargate.sgv3.docsapi.operations.FindOneByOneTextOperation;
import io.stargate.sgv3.docsapi.operations.FindOperation;
import io.stargate.sgv3.docsapi.operations.Operation;
import io.stargate.sgv3.docsapi.shredding.JsonType;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Base for resolvers that are {@link Filterable}, there are a number of commands like find,
 * findOne, updateOne that all have a filter.
 *
 * <p>There will be some re-use, and some customisation to work out.
 *
 * <p>T - type of the command we are resolving
 */
public class FilterableResolver<T extends Command & Filterable> implements CommandResolver<T> {

  private final FilterMatchRules<T> matchRules = new FilterMatchRules<>();

  private static final Object ID_GROUP = new Object();
  private static final Object SINGLE_TEXT_GROUP = new Object();

  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();
  private static final Object DYNAMIC_BOOL_GROUP = new Object();
  private static final Object DYNAMIC_NULL_GROUP = new Object();

  private static final Object EMPTY_GROUP = new Object();
  private boolean findOne;

  public FilterableResolver(boolean findOne) {
    this.findOne = findOne;
    // does not matter if it is findOne, match by _id will always be this operation
    matchRules.addMatchRule(this::findNoFilter, MatchStrategy.EMPTY);

    matchRules
        .addMatchRule(this::findById, MatchStrategy.STRICT)
        .matcher
        .capture(ID_GROUP)
        .compareValues("_id", ValueComparisonOperator.EQ, JsonType.STRING);

    if (findOne) {
      matchRules
          .addMatchRule(this::findByOneText, MatchStrategy.STRICT)
          .matcher
          .capture(SINGLE_TEXT_GROUP)
          .compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING);
    }

    // NOTE - can only do eq ops on fields until SAI changes
    matchRules
        .addMatchRule(this::findDynamic, MatchStrategy.GREEDY)
        .matcher
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP)
        .compareValues("*", ValueComparisonOperator.EQ, JsonType.NULL);
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

  @Override
  public Operation resolveCommand(CommandContext commandContext, T command) {
    return matchRules.apply(commandContext, command);
  }

  public Optional<FilteringOptions> getFilteringOption(T command) {
    if (findOne) {
      return Optional.of(new FilteringOptions(1, null));
    }
    return Optional.empty();
  }

  protected Operation findById(CommandContext commandContext, CaptureGroups<T> captures) {
    CapturePairLiteral capturePair =
        captures
            .getGroup(ID_GROUP)
            .orElseThrow(() -> new RuntimeException("Expected ID capture missing"))
            .getSingleLiteral();

    return new FindByIdOperation(commandContext, capturePair.literal.getTypedValue());
  }

  protected Operation findByOneText(CommandContext commandContext, CaptureGroups<T> captures) {
    CapturePairLiteral capturePair =
        captures
            .getGroup(SINGLE_TEXT_GROUP)
            .orElseThrow(() -> new RuntimeException("Expected single text field capture missing"))
            .getSingleLiteral();

    return new FindOneByOneTextOperation(
        commandContext, capturePair.path.getPath(), capturePair.literal.getTypedValue());
  }

  protected Operation findNoFilter(CommandContext commandContext, CaptureGroups<T> captures) {
    Optional<FilteringOptions> filteringOptions = getFilteringOption(captures.command);
    if (filteringOptions.isPresent()) {
      return new FindOperation(
          commandContext, filteringOptions.get().pagingState, filteringOptions.get().limit);
    }
    throw new RuntimeException("Options need to be returned for no filter reads");
  }

  protected Operation findDynamic(CommandContext commandContext, CaptureGroups<T> captures) {
    // NOTE TO ME TODO only have eq in the capture groups for now

    List<DBFilterBase> filters = new ArrayList<>();

    // there is probably a cleaner way to do this, will think again when we add more
    // also, it would be safer to have a test that all the captures the match rule had were consumed
    captures.consumeAllCaptures(
        DYNAMIC_TEXT_GROUP,
        pair ->
            filters.add(
                new TextFilter(
                    pair.path.getPath(),
                    MapFilterBase.Operator.EQ,
                    pair.literalOrList.safeGetLiteral().getTypedValue())));
    captures.consumeAllCaptures(
        DYNAMIC_BOOL_GROUP,
        pair ->
            filters.add(
                new BoolFilter(
                    pair.path.getPath(),
                    MapFilterBase.Operator.EQ,
                    pair.literalOrList.safeGetLiteral().getTypedValue())));
    captures.consumeAllCaptures(
        DYNAMIC_NUMBER_GROUP,
        pair ->
            filters.add(
                new NumberFilter(
                    pair.path.getPath(),
                    MapFilterBase.Operator.EQ,
                    pair.literalOrList.safeGetLiteral().getTypedValue())));
    captures.consumeAllCaptures(
        DYNAMIC_NULL_GROUP, pair -> filters.add(new IsNullFilter(pair.path.getPath())));
    Optional<FilteringOptions> filteringOptions = getFilteringOption(captures.command);
    if (filteringOptions.isPresent()) {
      return new FindDynamicQueryOperation(
          commandContext,
          filters,
          filteringOptions.get().pagingState,
          filteringOptions.get().limit);
    }
    throw new RuntimeException("Options need to be returned for filterable of non findOne");
  }
}
