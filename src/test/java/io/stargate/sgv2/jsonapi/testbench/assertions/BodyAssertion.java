package io.stargate.sgv2.jsonapi.testbench.assertions;

import io.stargate.sgv2.jsonapi.testbench.messaging.APIResponse;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

/**
 * Assertions that check the body of the response using a {@link Matcher} form hamcrest.
 *
 * <p>Example:
 *
 * <pre>
 *   return new BodyAssertion("data.documents", hasSize(expectedCount));
 * </pre>
 */
public record BodyAssertion(String bodyPath, Matcher<?> matcher)
    implements Describable, AssertionMatcher {

  @Override
  public void match(APIResponse apiResponse) {
    apiResponse.validatable().body(bodyPath(), matcher());
  }

  @Override
  public String describe() {
    var describable = new StringDescription();
    matcher.describeTo(describable);

    // called should truncate if it wants to limit it
    return "body('%s') - %s".formatted(bodyPath(), describable.toString());
  }
}
