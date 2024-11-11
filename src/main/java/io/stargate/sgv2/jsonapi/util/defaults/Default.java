package io.stargate.sgv2.jsonapi.util.defaults;

import java.util.function.Function;

public interface Default<T> {

  T defaultValue();

  T apply(T value);

  default <TSource> T apply(TSource source, Function<TSource, T> supplier) {
    return apply(source == null ? null : supplier.apply(source));
  }

  boolean isPresent(T value);

  interface Stringable<T> extends Default<T> {

    default boolean isPresent(String value) {
      return !(value == null || value.isBlank());
    }

    T applyToType(String stringValue);

    String applyToString(T objectValue);
  }
}
