package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;

public class Documents {

  public static AssertionMatcher count(JsonNode args) {
    var expectedCount = args.asInt();
    return new BodyAssertion("data.documents", hasSize(expectedCount));
  }

  public static AssertionMatcher isExactly(JsonNode args) {
    return new BodyAssertion("data.document", jsonEquals(args));
  }
}
