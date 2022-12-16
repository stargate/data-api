package io.stargate.sgv3.docsapi.commands.clauses.filter;

import io.stargate.sgv3.docsapi.commands.clauses.filter.ComparisonExpression.ComparisonMatcher;
import java.util.Optional;

/**
 * an operation we are applying against a context node <filter-operation> ::=
 * <filter-comparison-operation> | <filter-logical-unary-operation> | <filter-element-operation> |
 * <filter-array-operation>
 */
public abstract class FilterOperation {

  public static <T extends FilterOperation> ComparisonMatcher<FilterOperation> wrap(
      ComparisonMatcher<T> inner) {
    return new Matcher<T>(inner);
  }

  private static class Matcher<T extends FilterOperation>
      implements ComparisonMatcher<FilterOperation> {
    ComparisonMatcher<T> inner;

    public Matcher(ComparisonMatcher<T> inner) {
      this.inner = inner;
    }

    @Override
    public Optional<JsonLiteralOrList> match(FilterOperation t) {
      return inner.match((T) t);
    }
  }
}
