package io.stargate.sgv2.jsonapi.util.defaults;

public class BooleanProperty extends PropertyDefault<String, Boolean> {

  public BooleanProperty(String key, Boolean defaultValue) {
    this(key, Defaults.of(defaultValue));
  }

  protected BooleanProperty(String key, Default<Boolean> defaultValue) {
    super(key, defaultValue);
  }

  public static class Stringable
      extends PropertyDefault.StringablePropertyDefault<String, Boolean> {

    public Stringable(String key, Boolean value) {
      super(key, Defaults.ofStringable(value));
    }

    public Stringable(String key, DefaultBoolean wrapped) {
      super(key, Defaults.ofStringable(wrapped));
    }
  }
}
