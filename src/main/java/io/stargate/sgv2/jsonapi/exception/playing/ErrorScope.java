package io.stargate.sgv2.jsonapi.exception.playing;

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;

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
public interface ErrorScope {

  /** The NONE scope is used to represent the absence of a scope. */
  ErrorScope NONE = () -> "";

  /**
   * Implementing ENUM's must return a unique string that represents the scope.
   *
   * <p>Users of the scope should use {@link #safeScope()} rather than call this directly.
   *
   * <p>
   *
   * @return String representing the scope.
   */
  String scope();

  /**
   * Users of a scope should call this method to get the scope rather than {#link scope()}.
   *
   * <p>
   *
   * @return Returns the scope as non-null, SNAKE_CASE string.
   */
  default String safeScope() {
    return CharMatcher.whitespace().replaceFrom(Strings.nullToEmpty(scope()), '_').toUpperCase();
  }
}
