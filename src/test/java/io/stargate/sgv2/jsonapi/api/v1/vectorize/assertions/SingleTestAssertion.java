package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.v1.vectorize.TestResponse;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiSupportDef;
import org.hamcrest.SelfDescribing;
import org.hamcrest.StringDescription;
import org.junit.jupiter.api.DynamicNode;

import java.lang.reflect.Executable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.junit.jupiter.api.DynamicTest.stream;

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

    var original = (matcher instanceof Describable d) ?
      d.describe()
        :
        null;

    var truncated =  ( original != null && original.length() > 60) ?
         original.substring(0, 57) + "..."
        :
        original;

    var testDesc = truncated == null ?
      name()
      :
      "%s [%s]".formatted(name(), truncated);

    // if we truncated the description of the test, we then want to pipe to std out when running
    // because it will not be full in the test tree
    var stdoutMessage = (original != null && !Objects.equals(truncated, original)) ?
        "%s [%s]".formatted(name(), original)
        :
        null;

    return dynamicTest(testDesc, () -> {
          var resp = testResponse.get();
          if (resp == null) {
            throw new IllegalStateException("Response is null");
          }
          if (stdoutMessage != null) {
            System.out.printf(stdoutMessage);
          }
          run(resp);
        }
    );
  }
}
