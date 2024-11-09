package io.stargate.sgv2.jsonapi.util.defaults;

import java.util.Objects;
import java.util.function.Predicate;

public class DefaultBoolean extends Default<Boolean> {

  public DefaultBoolean(Boolean defaultValue) {
    super(defaultValue);
  }

  public DefaultBoolean(DefaultBoolean wrapped) {
    super(wrapped);
  }

  @Override
  public Boolean apply(Boolean t) {
    return coalesce(t, defaultValue);
  }

  @Override
  protected Predicate<Boolean> createIsPresent() {
    return Objects::nonNull;
  }

  private static Boolean coalesce(Boolean value, Boolean defaultValue) {
    return value == null ? defaultValue : value;
  }

  static class StringableDefaultBoolean extends StringableDefault<Boolean> {

    public StringableDefaultBoolean(Boolean value) {
      super(value);
    }

    public StringableDefaultBoolean(DefaultBoolean wrapped) {
      super(wrapped);
    }

    @Override
    protected Predicate<Boolean> createIsPresent() {
      return Objects::nonNull;
    }

    @Override
    Boolean apply(String stringValue) {
      // want to preserve the null and not get the default false from boolean
      var parsed =
          (stringValue == null || stringValue.isBlank()) ? null : Boolean.valueOf(stringValue);
      return apply(parsed);
    }

    @Override
    public Boolean apply(Boolean value) {
      return coalesce(value, defaultValue);
    }
  }
}
