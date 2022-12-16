package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause.Filterable;
import io.stargate.sgv3.docsapi.commands.clauses.filter.ComparisonExpression;
import io.stargate.sgv3.docsapi.commands.clauses.filter.ComparisonExpression.ComparisonMatcher;
import io.stargate.sgv3.docsapi.commands.clauses.filter.JsonLiteralOrList;
import io.stargate.sgv3.docsapi.commands.clauses.filter.OperatorExpression;
import io.stargate.sgv3.docsapi.commands.clauses.filter.ValueComparisonOperation;
import io.stargate.sgv3.docsapi.commands.clauses.filter.ValueComparisonOperator;
import io.stargate.sgv3.docsapi.shredding.JsonType;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Tests if a {@link FilterClause} matches a pattern.
 *
 * <p>The Matchers are defined with the Filter clauses, this class: - Provides a Fluent builder to
 * create match rules. - Associates matching with a Marker in a {@link Capture}, the Capture has the
 * the fluent builder on it. You make Capture to identify the filters you match to and pull the
 * params. - Runs the Captures against a Command, for now is very strict (see {@link
 * #apply(Command)} - Collects the parameters passed to the clauses that single match into a
 * {@CaptureGroup} and then into {@link CaptureGroups} where you can get them via the Marker
 *
 * <p>The defined Captures should be thread safe and re-usable for all API calls, the state for a
 * particular call goes into CaptureGroup.
 *
 * <p>See {@link FilterMatchRules#addMatchRule(BiFunction, MatchStrategy)} for how it is used.
 *
 * <p>T - {@link Command} to match against, must be {@link Filterable}
 */
public class FilterMatcher<T extends Command & Filterable> {

  private List<Capture> captures = new ArrayList<>();

  public enum MatchStrategy {
    EMPTY,
    STRICT, // every capture must match once and only once, every expression must match
    GREEDY, // capture groups can match zero or more times, every expression must match
  }

  private final MatchStrategy strategy;

  FilterMatcher(MatchStrategy strategy) {
    this.strategy = strategy;
  }

  public Optional<CaptureGroups<T>> apply(T command) {

    // TODO- only tests the ComparisonExpressions, needs to also test LogicalExpressions

    FilterClause filter = command.getFilter();

    // Each Capture can must be evaluate to True, once and only once
    // i.e. if we have a match for a single text field filter we should not
    // match for a filter that has two text fields.
    // the rule matches if all the predicates have a match.

    // AND every ComparisonExpression must match to a predicate.

    // All this means matchers have to exactly describe the query. e.g. a query with
    // three fields, two of them text and one a number.
    // That may be a problem for complex queries that need full dynamic CQL generation
    // We could handle that by adding config for process above ^
    // e.g. only some predicated match, or can match multiple times, or no all expressions have to
    // match

    // use ListIter to support removing matched predicates from the list.

    List<Capture> unmatchedCaptures = new ArrayList<>(captures);
    CaptureGroups.Builder<T> capturesBuilder = CaptureGroups.builder(command);
    if (strategy == MatchStrategy.EMPTY
        && (filter == null || filter.comparisonExpressions.isEmpty())) {
      return Optional.of(capturesBuilder.build());
    }
    List<ComparisonExpression> unmatchedExpressions = new ArrayList<>(filter.comparisonExpressions);

    ListIterator<ComparisonExpression> expressionIter = unmatchedExpressions.listIterator();

    while (expressionIter.hasNext()) {
      ComparisonExpression expression = expressionIter.next();

      // restart the capture iteration (excluding matched captures) for each expression we handle
      ListIterator<Capture> captureIter = unmatchedCaptures.listIterator();
      boolean captureMatch = false;
      while (!captureMatch && captureIter.hasNext()) {
        Capture capture = captureIter.next();

        var matchResult = capture.match(expression);

        if (matchResult.isPresent()) {
          // stop applying captures to this expression, strict and greedy strategy do not allow an
          // expression to capture multiple times.
          captureMatch = true;

          capturesBuilder
              .getCaptureGroupBuilder(capture.marker)
              .withCapture(expression.path, matchResult.get());

          switch (strategy) {
            case STRICT:
              captureIter.remove();
              expressionIter.remove();
              break;
            case GREEDY:
              expressionIter.remove();
              break;
            default:
              throw new RuntimeException(String.format("Unknown strategy %s", strategy));
          }
        }
      }
    }

    // these strategies should be abstracted if we have another one, only 2 for now.
    switch (strategy) {
      case STRICT:
        if (unmatchedCaptures.isEmpty() && unmatchedExpressions.isEmpty()) {
          // everything group and expression matched
          return Optional.of(capturesBuilder.build());
        }
        return Optional.empty();
      case EMPTY:
        // NONE is for no filter case which is handled above the while loop
        return Optional.empty();
      case GREEDY:
        if (unmatchedExpressions.isEmpty()) {
          // everything expression matched, some captures may not match
          return Optional.of(capturesBuilder.build());
        }
        return Optional.empty();
      default:
        throw new RuntimeException(String.format("Unknown strategy %s", strategy));
    }
  }

  /**
   * Start of the fluent API, create a Capture then add the matching
   *
   * <p>See {@link FilterMatchRules#addMatchRule(BiFunction, MatchStrategy)}}
   *
   * @param marker
   * @return
   */
  public Capture capture(Object marker) {
    final Capture newCapture = new Capture(marker);
    captures.add(newCapture);
    return newCapture;
  }

  // ========================================================
  // =================== Fluent API Below ===================
  // ========================================================

  /**
   * Capture provides a fluent API to build the matchers to apply to the filter.
   *
   * <p>**NOTE:** Is a non static class, it is bound to an instance of FilterMatcher to provide the
   * fluent API.
   */
  public final class Capture implements ComparisonMatcher<ComparisonExpression> {

    // marker used to identify capture groups later
    protected Object marker;

    // TODO - better management of the matcher being null
    private ComparisonMatcher<ComparisonExpression> matcher;

    protected Capture(Object marker) {
      this.marker = marker;
    }

    @Override
    public Optional<JsonLiteralOrList> match(ComparisonExpression t) {
      return matcher.match(t);
    }

    /**
     * The path is tested for equals against the specified type.
     *
     * <p>A shortcut for {@link #compareValues(String, ValueComparisonOperator, JsonType)}
     *
     * <p>e.g. <code>
     *  .eq("_id", JsonType.STRING);
     * </code>
     *
     * @param path
     * @param type
     * @return
     */
    // public FilterMatcher<T> valueComparison(String path, JsonType type){
    //     return compare(path, ValueComparisonOperator.EQ, type);
    // }

    public FilterMatcher<T> compareValues(
        String path, ValueComparisonOperator operator, JsonType type) {
      return compareValues(path, EnumSet.of(operator), type);
    }
    /**
     * The path is compared using an operator against a value of a type
     *
     * <p>e.g. <code>
     *  .compare("*", ValueComparisonOperator.GT, JsonType.NUMBER);
     * </code>
     *
     * @param path
     * @param type
     * @return
     */
    public FilterMatcher<T> compareValues(
        String path, EnumSet<ValueComparisonOperator> operators, JsonType type) {
      matcher =
          ComparisonExpression.match(
              path, OperatorExpression.match(ValueComparisonOperation.match(operators, type)));
      return FilterMatcher.this;
    }
  }
}
