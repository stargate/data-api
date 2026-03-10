package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.messaging.APIResponse;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;

/**
 * Assertions that check the body of the response using a {@link Matcher} form hamcrest.
 *
 * <p>
 * Example:
 * <pre>
 *   return new BodyAssertion("data.documents", hasSize(expectedCount));
 * </pre>
 * </p>
 */
public record BodyAssertion(
    String bodyPath,
    Matcher<?> matcher
) implements Describable, AssertionMatcher {

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