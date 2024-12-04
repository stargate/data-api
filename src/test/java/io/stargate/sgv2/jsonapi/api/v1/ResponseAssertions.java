package io.stargate.sgv2.jsonapi.api.v1;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.assertj.core.api.MapAssert;
import org.hamcrest.TypeSafeMatcher;

public class ResponseAssertions {

  public static TypeSafeMatcher<Map<String, ?>> responseIsFindSuccess() {
    return envelopeChecker(
        "responseIsFindSuccess", Presence.REQUIRED, Presence.FORBIDDEN, Presence.FORBIDDEN);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsFindAndSuccess() {
    return envelopeChecker(
        "responseIsFindAndSuccess", Presence.REQUIRED, Presence.REQUIRED, Presence.FORBIDDEN);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsFindSuccessOptionalStatus() {
    return envelopeChecker(
        "responseIsFindSuccessOptionalStatus",
        Presence.REQUIRED,
        Presence.OPTIONAL,
        Presence.FORBIDDEN);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsStatusOnly() {
    return envelopeChecker(
        "responseIsStatusOnly", Presence.FORBIDDEN, Presence.REQUIRED, Presence.FORBIDDEN);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsError() {
    return envelopeChecker(
        "responseIsError", Presence.FORBIDDEN, Presence.FORBIDDEN, Presence.REQUIRED);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsErrorWithStatus() {
    return envelopeChecker(
        "responseIsErrorWithStatus", Presence.FORBIDDEN, Presence.REQUIRED, Presence.REQUIRED);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsErrorWithOptionalStatus() {
    return envelopeChecker(
        "responseIsErrorWithStatus", Presence.FORBIDDEN, Presence.OPTIONAL, Presence.REQUIRED);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsWriteSuccess() {
    return envelopeChecker(
        "responseIsWriteSuccess", Presence.FORBIDDEN, Presence.REQUIRED, Presence.FORBIDDEN);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsPartialWriteSuccess() {
    return envelopeChecker(
        "responseIsPartialSuccess", Presence.FORBIDDEN, Presence.OPTIONAL, Presence.OPTIONAL);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsWritePartialSuccess() {
    return envelopeChecker(
        "responseIsWritePartialSuccess", Presence.FORBIDDEN, Presence.REQUIRED, Presence.REQUIRED);
  }

  public static TypeSafeMatcher<Map<String, ?>> responseIsDDLSuccess() {
    return envelopeChecker(
        "responseIsDDLSuccess", Presence.FORBIDDEN, Presence.REQUIRED, Presence.FORBIDDEN);
  }

  private static TypeSafeMatcher<Map<String, ?>> envelopeChecker(
      String message, Presence hasData, Presence hasStatus, Presence hasErrors) {

    var fieldMatchers =
        List.of(
            FieldMatcher.data(hasData),
            FieldMatcher.status(hasStatus),
            FieldMatcher.errors(hasErrors));

    final String msg =
        "%s: Response fields %s:%s, %s:%s, %s:%s"
            .formatted(
                message,
                Presence.REQUIRED,
                fieldMatchers.stream().filter(f -> f.presence == Presence.REQUIRED).toList(),
                Presence.OPTIONAL,
                fieldMatchers.stream().filter(f -> f.presence == Presence.OPTIONAL).toList(),
                Presence.FORBIDDEN,
                fieldMatchers.stream().filter(f -> f.presence == Presence.FORBIDDEN).toList());

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

  private enum Presence {
    REQUIRED,
    OPTIONAL,
    FORBIDDEN
  }

  private static class FieldMatcher {
    private final String name;
    private final Presence presence;
    private final Class<?> valueType;

    private FieldMatcher(String name, Presence presence, Class<?> valueType) {
      this.name = name;
      this.presence = presence;
      this.valueType = valueType;
    }

    static FieldMatcher data(Presence presence) {
      return new FieldMatcher("data", presence, Map.class);
    }

    static FieldMatcher status(Presence presence) {
      return new FieldMatcher("status", presence, Map.class);
    }

    static FieldMatcher errors(Presence presence) {
      return new FieldMatcher("errors", presence, List.class);
    }

    void match(MapAssert<String, ?> mapAssert) {
      switch (presence) {
        case REQUIRED ->
            mapAssert.hasEntrySatisfying(name, value -> assertThat(value).isInstanceOf(valueType));
        case FORBIDDEN -> mapAssert.doesNotContainKey(name);
        case OPTIONAL ->
            mapAssert.satisfies(
                m -> {
                  if (m.containsKey(name)) {
                    assertThat(m.get(name)).isInstanceOf(valueType);
                  }
                });
      }
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
