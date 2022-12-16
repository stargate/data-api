package io.stargate.sgv3.docsapi.commands.clauses.filter;

import java.util.EnumSet;
import java.util.function.Predicate;

public enum ValueComparisonOperator {
  EQ,
  GT,
  GTE,
  LT,
  LTE,
  NE;

  public static Predicate<ValueComparisonOperator> match(EnumSet<ValueComparisonOperator> matches) {
    return new Matcher(matches);
  }

  public Predicate<ValueComparisonOperator> match() {
    return new Matcher(EnumSet.of(this));
  }

  private static class Matcher extends EnumMatcher<ValueComparisonOperator> {
    public Matcher(EnumSet<ValueComparisonOperator> matches) {
      super(matches);
    }
  }
}
