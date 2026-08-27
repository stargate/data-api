package io.stargate.sgv2.jsonapi.testbench.assertions;

import io.stargate.sgv2.jsonapi.testbench.messaging.APIResponse;

/**
 * Contract for matching the result of an API call to an assertion.
 *
 * <p>This is the raw function to do the work, without any descriptive elements around it.
 */
@FunctionalInterface
public interface AssertionMatcher {

  /**
   * Match the response to the assertion.
   *
   * @param apiResponse response from the API
   * @throws AssertionError if the match fails
   */
  void match(APIResponse apiResponse);
}
