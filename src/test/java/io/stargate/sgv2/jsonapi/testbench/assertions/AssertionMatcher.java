package io.stargate.sgv2.jsonapi.testbench.assertions;

import io.stargate.sgv2.jsonapi.testbench.messaging.APIResponse;

/** Contract for running an assertion on the response from the API. */
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
