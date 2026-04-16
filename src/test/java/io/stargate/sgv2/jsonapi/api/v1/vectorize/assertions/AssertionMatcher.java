package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.messaging.APIResponse;

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
