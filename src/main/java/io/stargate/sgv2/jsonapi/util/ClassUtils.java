package io.stargate.sgv2.jsonapi.util;

import java.util.Objects;

public abstract class ClassUtils {

  public static String classSimpleName(Object object) {
    return classSimpleName(Objects.requireNonNull(object, "object must not be null").getClass());
  }

  public static String classSimpleName(Class<?> clazz) {
    var simpleName = clazz.getSimpleName();
    // will be empty if the class is anonymous
    return simpleName.isBlank() ? clazz.getName() : simpleName;
  }
}
