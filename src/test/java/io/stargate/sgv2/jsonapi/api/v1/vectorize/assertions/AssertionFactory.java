package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;

@FunctionalInterface
public interface AssertionFactory {

  ITAssertion create(JsonNode args);
}
