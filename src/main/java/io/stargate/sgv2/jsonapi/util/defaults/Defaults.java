package io.stargate.sgv2.jsonapi.util.defaults;

public abstract class Defaults {

  public static DefaultString of(String value) {
    return new DefaultString(value);
  }

  public static DefaultBoolean of(Boolean value) {
    return new DefaultBoolean(value);
  }

  public static DefaultBoolean.Stringable ofStringable(Boolean value) {
    return new DefaultBoolean.Stringable(value);
  }

  public static DefaultBoolean.Stringable ofStringable(DefaultBoolean value) {
    return new DefaultBoolean.Stringable(value);
  }
}
