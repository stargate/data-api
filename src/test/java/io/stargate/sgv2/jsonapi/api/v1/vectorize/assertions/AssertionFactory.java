package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;

@FunctionalInterface
public interface AssertionFactory {

  TestAssertion create(JsonNode args);
}
