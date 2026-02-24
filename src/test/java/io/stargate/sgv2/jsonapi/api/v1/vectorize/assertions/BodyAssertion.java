package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.APIResponse;
import org.hamcrest.Matcher;

public record BodyAssertion(
    String bodyPath,
    Matcher<?> matcher
) implements AssertionMatcher {

  public void match(APIResponse apiResponse) {

    apiResponse.validatableResponse().body(bodyPath(), matcher());
  }
}