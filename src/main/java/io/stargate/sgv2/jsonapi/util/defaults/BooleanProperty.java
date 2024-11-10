package io.stargate.sgv2.jsonapi.util.defaults;

public class BooleanProperty extends Property<String, Boolean> {

  public BooleanProperty(String key, Boolean defaultValue) {
    this(key, Defaults.of(defaultValue));
  }

  protected BooleanProperty(String key, DefaultBoolean defaultValue) {
    super(key, defaultValue);
  }

  public static class Stringable extends Property.Stringable<String, Boolean> {

    public Stringable(String key, Boolean defaultValue) {
      super(key, Defaults.ofStringable(defaultValue));
    }

    public Stringable(String key, DefaultBoolean.Stringable defaultValue) {
      super(key, defaultValue);
    }
  }
}
