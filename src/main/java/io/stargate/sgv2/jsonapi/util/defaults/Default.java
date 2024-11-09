package io.stargate.sgv2.jsonapi.util.defaults;

import java.util.function.Predicate;

public abstract class Default<T> {

  protected T defaultValue;
  final Predicate<T> isPresent;

  protected Default(T defaultValue) {
    this.defaultValue = defaultValue;

    this.isPresent = createIsPresent();
    if (this.isPresent == null) {
      throw new IllegalStateException("isPresent must not be null");
    }
  }

  protected Default(Default<T> wrapped) {
    this.defaultValue = wrapped.defaultValue;

    this.isPresent = createIsPresent();
    if (this.isPresent == null) {
      throw new IllegalStateException("isPresent must not be null");
    }
  }

  public T defaultValue() {
    return defaultValue;
  }

  public abstract T apply(T value);

  public boolean isPresent(T value) {
    return isPresent.test(value);
  }

  protected abstract Predicate<T> createIsPresent();

  public abstract static class StringableDefault<T> extends Default<T> {

    protected StringableDefault(T defaultValue) {
      super(defaultValue);
    }

    protected StringableDefault(Default<T> wrapped) {
      super(wrapped);
    }

    abstract T apply(String stringValue);
  }
}
