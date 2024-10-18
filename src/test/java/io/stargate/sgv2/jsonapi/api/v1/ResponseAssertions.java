package io.stargate.sgv2.jsonapi.api.v1;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.assertj.core.api.MapAssert;
import org.hamcrest.TypeSafeMatcher;

public class ResponseAssertions {

  public static TypeSafeMatcher<Map<String, ?>> responseIsFindSuccess() {
    return envelopeChecker("responseIsFindSuccess", true, false, false);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsFindAndSuccess() {
    return envelopeChecker("responseIsFindAndSuccess", true, true, false);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsStatusOnly() {
    return envelopeChecker("responseIsStatusOnly", false, true, false);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsError() {
    return envelopeChecker("responseIsError", false, false, true);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsWriteSuccess() {
    return envelopeChecker("responseIsWriteSuccess", false, true, false);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsWritePartialSuccess() {
    return envelopeChecker("responseIsWritePartialSuccess", false, true, true);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsDDLSuccess() {
    return envelopeChecker("responseIsDDLSuccess", false, true, false);
  }

  private static TypeSafeMatcher<Map<String, ?>> envelopeChecker(
      String message, boolean hasData, boolean hasStatus, boolean hasErrors) {

    var fieldMatchers =
        List.of(
            FieldMatcher.data(hasData),
            FieldMatcher.status(hasStatus),
            FieldMatcher.errors(hasErrors));

    final String msg =
        "%s: Response has fields %s, does not have fields %s"
            .formatted(
                message,
                fieldMatchers.stream().filter(f -> f.required).toList(),
                fieldMatchers.stream().filter(f -> !f.required).toList());

    return new TypeSafeMatcher<>() {
      @Override
      protected boolean matchesSafely(Map<String, ?> response) {

        var matcher = assertThat(response).as(msg);
        fieldMatchers.forEach(f -> f.match(matcher));
        return true;
      }

      @Override
      public void describeTo(org.hamcrest.Description description) {
        description.appendText(msg);
      }
    };
  }

  private static class FieldMatcher {
    private final String name;
    private final boolean required;
    private final Class<?> valueType;

    private FieldMatcher(String name, boolean required, Class<?> valueType) {
      this.name = name;
      this.required = required;
      this.valueType = valueType;
    }

    static FieldMatcher data(boolean required) {
      return new FieldMatcher("data", required, Map.class);
    }

    static FieldMatcher status(boolean required) {
      return new FieldMatcher("status", required, Map.class);
    }

    static FieldMatcher errors(boolean required) {
      return new FieldMatcher("errors", required, List.class);
    }

    void match(MapAssert<String, ?> mapAssert) {
      if (required) {
        mapAssert.hasEntrySatisfying(name, value -> assertThat(value).isInstanceOf(valueType));
      } else {
        mapAssert.doesNotContainKey(name);
      }
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
