package io.stargate.sgv2.jsonapi.testbench.assertions;

import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.testbench.testspec.TestCommand;

/**
 * Assertions that check the `document` or `documents` in the `data` field of the API result.
 *
 * <p>See {@link TestAssertion}
 */
public class Documents {

  static {
    AssertionFactory.REGISTRY.register(Documents.class);
  }

  /**
   * Check the count of the `data.documents` array.
   *
   * <p>Assertion factory, see {@link AssertionFactory.AssertionMatcherFactory}
   */
  public static AssertionMatcher count(TestCommand testCommand, JsonNode args) {
    var expectedCount = args.asInt();
    return new BodyAssertion("data.documents", hasSize(expectedCount));
  }

  /**
   * Check that the `data.document` matches the given JSON.
   *
   * <p>Assertion factory, see {@link AssertionFactory.AssertionMatcherFactory}
   */
  public static AssertionMatcher isExactly(TestCommand testCommand, JsonNode args) {
    return new BodyAssertion("data.document", jsonEquals(args));
  }
}
