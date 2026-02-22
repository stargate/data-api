package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;

public class Status {
  public static TestAssertion isExactly(JsonNode args) {
    return new BodyAssertion("status", jsonEquals(args));
  }
}
