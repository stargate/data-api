package io.stargate.sgv2.jsonapi.service.operation.query;

/**
 * Interface that describes how a filter behaves.
 *
 * <p>Any behaviour about a filter should be described in this interface so that decisions about
 * warnings etc are (as much as possible) based on the interface and not understanding what the
 * filter is.
 *
 * <p>Implementations should return a result based on the configuration of that filter, not the
 * general case for the type of filter. i.e. an `IN` filter with a single value will return `true`
 * for `isExactMatch()`, but false if there are more than one value to test.
 */
public interface FilterBehaviour {

  /**
   * Called to test if the filter will match a single value only, e.g. an equals op
   *
   * <p>Another example is an
   *
   * @return <code>true</code> if the filter will match a single value, <code>false</code> otherwise
   */
  boolean filterIsExactMatch();

  /**
   * Called to test if the filter will match contiguous slice of ordered values, e.g. >, >=, <, <=
   *
   * @return <code>true</code> if the filter will match a slice of values, <code>false</code>
   *     otherwise
   */
  boolean filterIsSlice();

  /**
   * Helper record to hold all the settings for the interface in one place to make implementation
   * easier.
   *
   * @param isExactMatch
   * @param isSlice
   */
  record Behaviour(boolean isExactMatch, boolean isSlice) implements FilterBehaviour {
    @Override
    public boolean filterIsExactMatch() {
      return isExactMatch;
    }

    @Override
    public boolean filterIsSlice() {
      return isSlice;
    }
  }
}
