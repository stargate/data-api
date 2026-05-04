package io.stargate.sgv2.jsonapi.testbench.assertions;

import io.stargate.sgv2.jsonapi.testbench.messaging.APIResponse;
import org.jspecify.annotations.NonNull;

public record DescribableAssertionMatcher(String description, AssertionMatcher matcher)
    implements Describable, AssertionMatcher {

  public static DescribableAssertionMatcher described(
      String description, AssertionMatcher matcher) {
    return new DescribableAssertionMatcher(description, matcher);
  }

  @Override
  public void match(APIResponse apiResponse) {
    matcher.match(apiResponse);
  }

  @Override
  public String describe() {
    return toString();
  }

  @Override
  public @NonNull String toString() {
    return description();
  }
}
