package io.stargate.sgv2.jsonapi.util.defaults;

public class DefaultBoolean extends DefaultBase<Boolean> {

  DefaultBoolean(Boolean defaultValue) {
    super(defaultValue);
  }

  DefaultBoolean(DefaultBoolean wrapped) {
    super(wrapped);
  }

  @Override
  public Boolean apply(Boolean value) {
    return value == null ? defaultValue : value;
  }

  public static class Stringable extends DefaultBoolean implements Default.Stringable<Boolean> {

    Stringable(Boolean value) {
      super(value);
    }

    Stringable(DefaultBoolean wrapped) {
      super(wrapped);
    }

    @Override
    public Boolean applyToType(String stringValue) {
      return apply(
          (stringValue == null || stringValue.isBlank()) ? null : Boolean.valueOf(stringValue));
    }

    @Override
    public String applyToString(Boolean objectValue) {
      return apply(objectValue).toString();
    }
  }
}
