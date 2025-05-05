package io.stargate.sgv2.jsonapi.util.defaults;

public class DefaultString extends DefaultBase<String> {

  DefaultString(String defaultValue) {
    super(defaultValue);
  }

  DefaultString(DefaultString wrapped) {
    super(wrapped);
  }

  @Override
  public String apply(String value) {
    return value == null || value.isBlank() ? defaultValue : value;
  }
}
