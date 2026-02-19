package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public record TestResult(
    TestCase testItem,
    ObjectNode actualRequest,
    JsonNode response,
    AssertionError error
) {

  public boolean failed(){
    return error == null;
  }
}
