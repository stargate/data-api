package io.stargate.sgv3.docsapi.commands.clauses.filter;

import java.util.EnumSet;
import java.util.function.Predicate;

/*
 * Base to use for the Enums in the filter.
 *
 */
abstract class EnumMatcher<T extends Enum<T>> implements Predicate<T> {

  private EnumSet<T> matches;

  EnumMatcher(EnumSet<T> matches) {
    this.matches = matches;
  }

  @Override
  public boolean test(T t) {
    return matches.contains(t);
  }
}
