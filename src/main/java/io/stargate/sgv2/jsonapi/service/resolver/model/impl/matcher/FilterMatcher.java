package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ValueComparisonOperator;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * This class matches the filter clauses against the filter match rules defined. The match rules
 * will be defined in order of preference, so first best match will be used for query processing
 *
 * @param <T> should be a {@link Command} type, which also implements {@link Filterable}
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
    FilterClause filter = command.filterClause();
    List<Capture> unmatchedCaptures = new ArrayList<>(captures);
    CaptureGroups captures = new CaptureGroups(command);
    if (strategy == MatchStrategy.EMPTY) {
      if (filter == null || filter.comparisonExpressions().isEmpty()) {
        return Optional.of(captures);
      } else {
        return Optional.empty();
      }
    }

    if (filter == null) {
      return Optional.empty();
    }

    List<ComparisonExpression> unmatchedExpressions =
        new ArrayList<>(filter.comparisonExpressions());
    ListIterator<ComparisonExpression> expressionIter = unmatchedExpressions.listIterator();
    while (expressionIter.hasNext()) {
      ComparisonExpression comparisonExpression = expressionIter.next();
      ListIterator<Capture> captureIter = unmatchedCaptures.listIterator();
      while (captureIter.hasNext()) {
        Capture capture = captureIter.next();
        List<FilterOperation> matched = capture.match(comparisonExpression);
        if (!matched.isEmpty()) {
          captures.getGroup(capture.marker).withCapture(comparisonExpression.path(), matched);
          switch (strategy) {
            case STRICT:
              captureIter.remove();
              expressionIter.remove();
              break;
            case GREEDY:
              expressionIter.remove();
              break;
          }
          break;
        }
      }
    }
    // these strategies should be abstracted if we have another one, only 2 for now.
    switch (strategy) {
      case STRICT:
        if (unmatchedCaptures.isEmpty() && unmatchedExpressions.isEmpty()) {
          // everything group and expression matched
          return Optional.of(captures);
        }
        break;
      case GREEDY:
        if (unmatchedExpressions.isEmpty()) {
          // everything expression matched, some captures may not match
          return Optional.of(captures);
        }
        break;
    }
    return Optional.empty();
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

  /**
   * Capture provides a fluent API to build the matchers to apply to the filter.
   *
   * <p>**NOTE:** Is a non static class, it is bound to an instance of FilterMatcher to provide the
   * fluent API.
   */
  public final class Capture {

    private Object marker;
    private String matchPath;
    private EnumSet operators;
    private JsonType type;

    protected Capture(Object marker) {
      this.marker = marker;
    }

    public List<FilterOperation> match(ComparisonExpression t) {
      return t.match(matchPath, operators, type);
    }

    /**
     * A shortcut for {@link #compareValues(String, ValueComparisonOperator, JsonType)} e.g. <code>
     *  .eq("_id", JsonType.STRING);
     * </code>
     *
     * @param path
     * @param type
     * @return
     */
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
      this.matchPath = path;
      this.operators = operators;
      this.type = type;
      return FilterMatcher.this;
    }
  }
}
