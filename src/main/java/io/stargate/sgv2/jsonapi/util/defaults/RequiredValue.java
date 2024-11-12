package io.stargate.sgv2.jsonapi.util.defaults;

public class RequiredValue<T> implements Default<T> {

  public RequiredValue() {}

  @Override
  public T defaultValue() {
    throw new UnsupportedOperationException("RequiredValue");
  }

  @Override
  public T apply(T value) {
    if (!isPresent(value)) {
      throw new RequiredValueMissingException();
    }
    return value;
  }

  @Override
  public boolean isPresent(T value) {
    return value != null;
  }

  public static class RequiredValueMissingException extends RuntimeException {
    public RequiredValueMissingException() {
      super("Required value is missing");
    }

    public RequiredValueMissingException(String message) {
      super(message);
    }
  }
}
