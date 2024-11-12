package io.stargate.sgv2.jsonapi.util.defaults;

public class StringProperty extends Property<String, String> {

  public StringProperty(String key, String defaultValue) {
    this(key, Defaults.of(defaultValue));
  }

  protected StringProperty(String key, Default<String> defaultValue) {
    super(key, defaultValue);
  }
}
