package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.APIResponse;
import org.hamcrest.Matcher;

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
) implements AssertionMatcher {

  public void match(APIResponse apiResponse) {
    apiResponse.validatableResponse().body(bodyPath(), matcher());
  }
}