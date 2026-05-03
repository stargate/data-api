package io.stargate.sgv2.jsonapi.testbench.assertions;

import java.lang.reflect.Method;

public record AssertionName(String typeName, String funcName) {

  private static final String PACKAGE = "io.stargate.sgv2.jsonapi.testbench.assertions";

  public AssertionName {
    typeName = typeName.toLowerCase();
    funcName = funcName.toLowerCase();
  }

  public static AssertionName from(String fullKey) {

    int pos = fullKey.indexOf('.');
    if (pos < 0) {
      throw new IllegalArgumentException("fullKey must have a dot: " + fullKey);
    }
    var type = fullKey.substring(0, pos);
    var func = fullKey.substring(pos + 1);
    return new AssertionName(type, func);
  }

  public static String properName(Class<?> clazz, Method method) {
    return clazz.getSimpleName() + '.' + method.getName();
  }

  public static String properName(Method method) {
    return method.getName();
  }

  public String properClassName() {
    return PACKAGE + "." + Character.toUpperCase(typeName.charAt(0)) + typeName.substring(1);
  }

  public String normalisedKey() {
    return typeName + "." + funcName;
  }
}
