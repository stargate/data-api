package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.DynamicTestExecutable;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestRunResponse;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.testrun.TestUri;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DynamicNode;

public record SingleTestAssertion(String name, JsonNode args, AssertionMatcher matcher)
    implements TestAssertion {

  public void run(TestRunResponse testResponse) {

    try {
      matcher.match(testResponse.apiResponse());
    } catch (AssertionError e) {
//      System.out.printf("Failed Assertion: name=%s, args=%s", name, args);
      throw e;
    } catch (Exception e) {
//      System.out.printf("Error In Assertion: name=%s, args=%s", name, args);
      throw e;
    }
  }

  @Override
  public DynamicNode testNodes(
      TestUri.Builder uriBuilder, AtomicReference<TestRunResponse> testResponse) {

    var matcherDesc = (matcher instanceof Describable d) ? d.describe() : "";

    var executable =
        new DynamicTestExecutable(
            "%s [%s]".formatted(name(), matcherDesc),
            uriBuilder.addSegment(TestUri.Segment.ASSERTION, name()),
            () -> {
              var resp = testResponse.get();
              if (resp == null) {
                throw new IllegalStateException("Response is null");
              }
              run(resp);
            });

    return executable.testNode();
  }
}
