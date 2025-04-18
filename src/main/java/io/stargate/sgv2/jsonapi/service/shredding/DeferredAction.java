package io.stargate.sgv2.jsonapi.service.shredding;

import java.util.List;

/**
 * Marker interface for a class that can be associated with a {@link NamedValue} that is in the
 * {@link io.stargate.sgv2.jsonapi.service.shredding.NamedValue.NamedValueState#DEFERRED} state.
 *
 * <p>As a marker interface this does not have any methods on it, it is here so the NamedValue can
 * manage its action. Typically, the implications accept a callback function for success and failure
 * that then updates the NamedValue. See {@link
 * io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingDeferredAction} for example.
 *
 * <p>See also {@link Deferrable}
 */
public interface DeferredAction {

  /**
   * Filters the {@link NamedValue#deferredValue} 's from the deferred values that are of the given
   * class.
   *
   * <p>Use when you have a list of named values, and you want to get all the ones waiting for the
   * same action:
   *
   * <pre>
   *  var embeddingActions = ValueAction.filteredActions(EmbeddingAction.class, allDeferredValues);
   * </pre>
   *
   * @param clazz Class of the {@link DeferredAction} to filter out, actions of a different class
   *     are ignored.
   * @param allDeferredValues The {@link NamedValue}s that have a {@link
   *     NamedValue#deferredAction()} to filter.
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
