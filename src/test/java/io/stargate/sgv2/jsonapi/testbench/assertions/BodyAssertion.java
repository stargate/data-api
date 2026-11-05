package io.stargate.sgv2.jsonapi.testbench.assertions;

import io.stargate.sgv2.jsonapi.testbench.messaging.APIResponse;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

/**
 * Assertions that check the body of the response using a {@link Matcher} from hamcrest.
 *
 * <p>Example:
 *
 * <pre>
 *   return new BodyAssertion("data.documents", hasSize(expectedCount));
 * </pre>
 *
 * <p>So this is a very re-usable record, as most assertions we want to create run a matcher against
 * the body of the response by calling {@link
 * io.restassured.response.ValidatableResponse#body(String, Matcher)}. This record makes it easy to
 * make a matcher from hamcrest, work out the description, and plug it into the {@link
 * TestAssertion} structure so we can run it in the dynamic tests we build
 *
 * @param bodyPath Path to the body to check, e.g. "data.documents"
 * @param matcher Matcher to use to check the body, e.g. hasSize(expectedCount)
 */
public record BodyAssertion(String bodyPath, Matcher<?> matcher)
    implements Describable, AssertionMatcher {

  @Override
  public void match(APIResponse apiResponse) {
    apiResponse.validatable().body(bodyPath(), matcher());
  }

  /** Describes the assertion, based on the path and the matcher. */
  @Override
  public String describe() {
    var describable = new StringDescription();
    // get hamcrest to describe the matcher it will run
    matcher.describeTo(describable);

    // caller should truncate if it wants to limit it
    // example: "body('data.documents') - a collection with size <3>"
    return "body('%s') - %s".formatted(bodyPath(), describable.toString());
  }
}
