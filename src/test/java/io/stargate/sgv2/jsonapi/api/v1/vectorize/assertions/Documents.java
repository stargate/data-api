package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestCommand;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;

/**
 * Assertions that check the `document` or `documents` in the `data` field
 * of the API result.
 * <p>
 * See {@link TestAssertion}
 * </p>
 */
public class Documents {

  static {
    AssertionMatcher.FACTORY_REGISTRY.register(Documents.class);
  }

  public static AssertionMatcher count(TestCommand testCommand, JsonNode args) {
    var expectedCount = args.asInt();
    return new BodyAssertion("data.documents", hasSize(expectedCount));
  }

  public static AssertionMatcher isExactly(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("data.document", jsonEquals(args));
  }
}
