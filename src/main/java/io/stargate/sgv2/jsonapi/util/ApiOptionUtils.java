package io.stargate.sgv2.jsonapi.util;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility methods for working with options on the API.
 *
 * <p>When working with properties such as those on in the Options for a command we have to do deal
 * with:
 *
 * <ul>
 *   <li>The options object is null - user did not send an options field.
 *   <li>The object is no null, but the property is null - user did not send the field or the field
 *       is JSON null .
 *   <li>The object is not null and the property is not null - user sent the field.
 * </ul>
 *
 * So we need to either get the actual value the user provided, or a default. This results in a
 * bunch of repeated code.
 */
public abstract class ApiOptionUtils {

  private ApiOptionUtils() {
    // prevent instantiation
  }

  public static <T, R> R getOrDefault(T source, Function<T, R> getter, R defaultValue) {
    if (source == null) {
      return defaultValue;
    }
    R value = getter.apply(source);
    return value == null ? defaultValue : value;
  }

  public static <T, R> R getOrGetDefault(
      T source, Function<T, R> getter, Supplier<R> defaultSupplier) {
    if (source == null) {
      return defaultSupplier.get();
    }
    R value = getter.apply(source);
    return value == null ? defaultSupplier.get() : value;
  }
}
