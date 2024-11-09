package io.stargate.sgv2.jsonapi.util.defaults;

public abstract class Defaults {

  public static DefaultBoolean of(Boolean value) {
    return new DefaultBoolean(value);
  }

  public static DefaultBoolean.StringableDefaultBoolean ofStringable(Boolean value) {
    return new DefaultBoolean.StringableDefaultBoolean(value);
  }

  public static DefaultBoolean.StringableDefaultBoolean ofStringable(DefaultBoolean value) {
    return new DefaultBoolean.StringableDefaultBoolean(value);
  }
}
