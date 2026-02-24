package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.APIResponse;

public interface AssertionMatcher {

  void match(APIResponse apiResponse);
}
