package io.stargate.sgv2.jsonapi.exception.playing;

/**
 * Interface for any enum that represents an error scope to implement.
 *
 * <p>This is used to group errors together in a {@link ErrorFamily}.
 *
 * <p>This interface is used because multiple ENUM's define the scopes, the interface creates a way
 * to treat the values from the different ENUM's in a consistent way.
 *
 * <p>See {@link APIException}
 */
@FunctionalInterface
public interface ErrorScope {

  /** The NONE scope is used to represent the absence of a scope. */
  ErrorScope NONE = () -> "";

  /**
   * Implementing ENUM's must return a unique string that represents the scope.
   *
   * @return String representing the scope.
   */
  String scope();
}
