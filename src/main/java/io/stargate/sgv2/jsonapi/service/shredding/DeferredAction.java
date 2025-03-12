package io.stargate.sgv2.jsonapi.service.shredding;

import java.util.List;

public interface DeferredAction {

  /**
   * Filters the {@link NamedValue#deferredValue} 's from the deferred values that are of the given
   * class.
   *
   * <p>
   *
   * @param clazz Class of the {@link DeferredAction} to filter out, non null actions of a different
   *     class are ignored.
   * @param allDeferredValues The deferred values to filter
   * @return A list of the deferred values that are of the given class
   * @param <T> The type of the {@link DeferredAction} to filter out
   * @throws IllegalStateException if the values in allDeferredValues are not deferred, this comes
   *     from {@link NamedValue#deferredValue} call
   */
  static <T extends DeferredAction> List<T> filtered(
      Class<T> clazz, List<? extends Deferred> allDeferredValues) {
    if (allDeferredValues == null || allDeferredValues.isEmpty()) {
      return List.of();
    }

    return allDeferredValues.stream()
        .map(Deferred::deferredAction)
        .filter(clazz::isInstance)
        .map(clazz::cast)
        .toList();
  }
}
