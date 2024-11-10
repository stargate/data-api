package io.stargate.sgv2.jsonapi.util.defaults;

import java.util.Objects;
import java.util.function.Predicate;

public abstract class DefaultBase<T> implements Default<T> {

  protected final T defaultValue;
  protected final Predicate<T> isPresent;

  protected DefaultBase(T defaultValue) {
    this.defaultValue = defaultValue;

    this.isPresent = createIsPresent();
    if (this.isPresent == null) {
      throw new IllegalStateException("isPresent must not be null");
    }
  }

  protected DefaultBase(DefaultBase<T> wrapped) {
    this.defaultValue = wrapped.defaultValue;

    this.isPresent = createIsPresent();
    if (this.isPresent == null) {
      throw new IllegalStateException("isPresent must not be null");
    }
  }

  protected Predicate<T> createIsPresent() {
    return Objects::nonNull;
  }

  @Override
  public T defaultValue() {
    return defaultValue;
  }

  @Override
  public boolean isPresent(T value) {
    return isPresent.test(value);
  }
}
