package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import java.util.*;

/**
 * This class matches the filter clauses against the filter match rules defined. The match rules
 * will be defined in order of preference, so first best match will be used for query processing
 *
 * @param <T> should be a {@link Command} type, which also implements {@link Filterable}
 */
public class FilterMatcher<T extends Command & Filterable> {

  private final List<Capture> captures = new ArrayList<>();

  public enum MatchStrategy {
    EMPTY,
    STRICT, // every capture must match once and only once, every expression must match
    GREEDY, // capture groups can match zero or more times, every expression must match
  }

  private final MatchStrategy strategy;

  FilterMatcher(MatchStrategy strategy) {
    this.strategy = strategy;
  }

  public Optional<CaptureGroups<T>> apply(CommandContext<?> ctx, T command) {
    final FilterClause filter = command.filterClause(ctx);
    // construct a default CaptureGroups, with default AND relation, empty captureGroupsList,
    // empty captureGroupMap
    CaptureGroups<T> captureGroups = new CaptureGroups<>(DBLogicalExpression.DBLogicalOperator.AND);
    if (strategy == MatchStrategy.EMPTY) {
      if (filter == null || filter.isEmpty()) {
        return Optional.of(captureGroups);
      } else {
        return Optional.empty();
      }
    }
    if (filter == null) {
      return Optional.empty();
    }
    List<Capture> unmatchedCaptures = new ArrayList<>(captures);
    final MatchStrategyCounter matchStrategyCounter =
        new MatchStrategyCounter(unmatchedCaptures.size(), filter.size());
    // capture recursively, resolve logicalExpression to captureGroups
    captureRecursive(
        captureGroups, filter.logicalExpression(), unmatchedCaptures, matchStrategyCounter);
    // apply strategy to the resolved root captureGroups
    return matchStrategyCounter.applyStrategy(strategy, captureGroups);
  }

  private void captureRecursive(
      CaptureGroups currentCaptureGroups,
      LogicalExpression expression,
      List<Capture> unmatchedCaptures,
      MatchStrategyCounter matchStrategyCounter) {

    // recursively resolve logicalExpression to captureGroups
    for (LogicalExpression innerLogicalExpression : expression.logicalExpressions) {
      CaptureGroups innerCaptureGroups =
          currentCaptureGroups.addSubCaptureGroups(
              new CaptureGroups<>(
                  DBLogicalExpression.DBLogicalOperator.fromLogicalOperator(
                      innerLogicalExpression.getLogicalRelation())));
      captureRecursive(
          innerCaptureGroups, innerLogicalExpression, unmatchedCaptures, matchStrategyCounter);
    }

    // resolve current level of comparisonExpressions to captureGroup
    for (ComparisonExpression comparisonExpression : expression.comparisonExpressions) {
      ListIterator<Capture> captureIter = unmatchedCaptures.listIterator();
      while (captureIter.hasNext()) {
        Capture capture = captureIter.next();
        List<FilterOperation<?>> matched = capture.match(comparisonExpression);
        if (!matched.isEmpty()) {
          currentCaptureGroups
              .getGroup(capture.marker)
              .addCapture(comparisonExpression.getPath(), matched);
          switch (strategy) {
            case STRICT:
              captureIter.remove();
              matchStrategyCounter.strictMatch();
              break;
            case GREEDY:
              matchStrategyCounter.greedyMatch();
              break;
          }
          break;
        }
      }
    }
  }

  /**
   * Start of the fluent API, create a Capture then add the matching
   *
   * <p>See {@link FilterMatchRules #addMatchRule(BiFunction, MatchStrategy)}}
   *
   * @param marker marker
   * @return capture
   */
  public Capture capture(Object marker) {
    final Capture newCapture = new Capture(marker);
    captures.add(newCapture);
    return newCapture;
  }

  /**
   * Capture provides a fluent API to build the matchers to apply to the filter.
   *
   * <p>**NOTE:** Is a non-static class, it is bound to an instance of FilterMatcher to provide the
   * fluent API.
   */
  public final class Capture {

    private Object marker;
    private String matchPath;
    private Set<? extends FilterOperator> operators;
    private JsonType type;
    // boolean value to indicate if the match is for a map, set or list.
    // This can be captured by a table filter against map/set/list column.
    private boolean targetIsMapSetList;

    protected Capture(Object marker) {
      this.marker = marker;
    }

    /**
     * Method to see if the comparisonExpression matches the capture. If so it will return a list of
     * FilterOperation that satisfy the match, If not it will return an empty list.
     */
    public List<FilterOperation<?>> match(ComparisonExpression t) {
      return t.match(matchPath, operators, type, targetIsMapSetList);
    }

    /**
     * Populate the current capture with the path, operators, JsonType. Return the current
     * FilterMatcher to allow for fluent API.
     */
    public FilterMatcher<T> compareValues(
        String path, EnumSet<? extends FilterOperator> operators, JsonType type) {
      this.matchPath = path;
      this.operators = operators;
      this.type = type;
      this.targetIsMapSetList = false;
      return FilterMatcher.this;
    }

    /**
     * Populate the current capture with the path, operators, JsonType and set
     * captureOfMapSetListComponent to indicate the capture against table map/set/list column.
     * Return the current FilterMatcher to allow for fluent API.
     */
    public FilterMatcher<T> compareMapSetListFilterValues(
        String path, Set<? extends FilterOperator> operators, JsonType type) {
      this.matchPath = path;
      this.operators = operators;
      this.type = type;
      this.targetIsMapSetList = true;
      return FilterMatcher.this;
    }
  }

  public final class MatchStrategyCounter {

    private int unmatchedCaptureCount;
    private int unmatchedComparisonExpressionCount;

    public MatchStrategyCounter(int unmatchedCaptureCount, int unmatchedComparisonExpressionCount) {
      this.unmatchedCaptureCount = unmatchedCaptureCount;
      this.unmatchedComparisonExpressionCount = unmatchedComparisonExpressionCount;
    }

    public void strictMatch() {
      unmatchedCaptureCount--;
      unmatchedComparisonExpressionCount--;
    }

    public void greedyMatch() {
      unmatchedComparisonExpressionCount--;
    }

    public Optional<CaptureGroups<T>> applyStrategy(
        MatchStrategy strategy, CaptureGroups<T> captureGroups) {
      // these strategies should be abstracted if we have another one, only 2 for now.
      switch (strategy) {
        case STRICT:
          if (unmatchedCaptureCount == 0 && unmatchedComparisonExpressionCount == 0) {
            // everything group and expression matched
            return Optional.of(captureGroups);
          }
          break;
        case GREEDY:
          if (unmatchedComparisonExpressionCount == 0) {
            // everything expression matched, some captures may not match
            return Optional.of(captureGroups);
          }
          break;
      }
      return Optional.empty();
    }
  }
}
