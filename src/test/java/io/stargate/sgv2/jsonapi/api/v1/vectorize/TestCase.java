package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.node.ObjectNode;

public record TestCase(

    String name,
    TestRequest request,
    ObjectNode asserts,
    @JsonProperty("$include")
    String include) {}
