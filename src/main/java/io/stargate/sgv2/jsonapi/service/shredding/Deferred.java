package io.stargate.sgv2.jsonapi.service.shredding;

import static io.stargate.sgv2.jsonapi.util.ClassUtils.classSimpleName;

public interface Deferred {

  DeferredAction deferredAction();

  default void checkCompleted(boolean isCompleted, String context) {
    if (!isCompleted) {
      throw new IllegalStateException(
          "Deferred value %s has not completed for context: %s"
              .formatted(classSimpleName(getClass()), context));
    }
  }

  default boolean maybeCompleted(boolean isCompleted, String context) {
    if (isCompleted) {
      throw new IllegalStateException(
          "Deferred value %s is already completed for context: %s"
              .formatted(classSimpleName(getClass()), context));
    }
    return true;
  }
}
