package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.JsonNode;

public record TestAssertion(String name, JsonNode assertion) {}
