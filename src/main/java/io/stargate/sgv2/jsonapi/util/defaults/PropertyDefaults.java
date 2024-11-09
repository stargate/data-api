package io.stargate.sgv2.jsonapi.util.defaults;

public abstract class PropertyDefaults {

  public static BooleanProperty of(String key, Boolean defaultValue) {
    return new BooleanProperty(key, defaultValue);
  }

  public static BooleanProperty.Stringable ofStringable(String key, Boolean defaultValue) {
    return new BooleanProperty.Stringable(key, defaultValue);
  }

  public static BooleanProperty.Stringable ofStringable(String key, DefaultBoolean defaultValue) {
    return new BooleanProperty.Stringable(key, defaultValue);
  }
}
