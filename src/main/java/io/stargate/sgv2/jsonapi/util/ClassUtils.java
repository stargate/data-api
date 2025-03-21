package io.stargate.sgv2.jsonapi.util;

public abstract class ClassUtils {

  public static String classSimpleName(Class<?> clazz) {
    var simpleName = clazz.getSimpleName();
    // will be empty if the class is anonymous
    return simpleName.isBlank() ? clazz.getName() : simpleName;
  }
}
