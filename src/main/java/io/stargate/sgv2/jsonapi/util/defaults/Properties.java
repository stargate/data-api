package io.stargate.sgv2.jsonapi.util.defaults;

public abstract class Properties {

  public static StringProperty ofRequired(String key) {
    return new StringProperty(key, new RequiredValue<>());
  }

  public static StringProperty of(String key, String defaultValue) {
    return new StringProperty(key, defaultValue);
  }

  public static StringProperty of(String key, DefaultString defaultValue) {
    return new StringProperty(key, defaultValue);
  }

  public static BooleanProperty of(String key, Boolean defaultValue) {
    return new BooleanProperty(key, defaultValue);
  }

  public static BooleanProperty.Stringable ofStringable(String key, Boolean defaultValue) {
    return new BooleanProperty.Stringable(key, defaultValue);
  }

  public static BooleanProperty.Stringable ofStringable(String key, DefaultBoolean defaultValue) {
    return new BooleanProperty.Stringable(key, Defaults.ofStringable(defaultValue));
  }
}
