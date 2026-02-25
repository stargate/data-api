package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestResponse;
import org.junit.jupiter.api.DynamicNode;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

public record SingleTestAssertion(
    String name,
    JsonNode args,
    AssertionMatcher matcher
) implements TestAssertion {

  public void run(TestResponse testResponse) {

    try {
      matcher.match(testResponse.apiResponse());
    } catch (AssertionError e) {
      System.out.printf("Failed Assertion: name=%s, args=%s", name, args);
      throw e;
    } catch (Exception e) {
      System.out.printf("Error In Assertion: name=%s, args=%s", name, args);
      throw e;
    }
  }

  @Override
  public DynamicNode testNodes(AtomicReference<TestResponse> testResponse) {

    return dynamicTest(name(), () -> {
          var resp = testResponse.get();
          if (resp == null) {
            throw new IllegalStateException("Response is null");
          }
          run(resp);
        }
    );
  }
}
