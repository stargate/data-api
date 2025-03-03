package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.service.operation.embeddings.EmbeddingAction;

import java.util.List;

public interface ValueAction {


  /**
   * Filters the {@link NamedValue#valueAction} 's from the deferred values that are of the
   * given class.
   * <p>
   * @param clazz Class of the {@link ValueAction} to filter out, non null actions of a different class are ignored.
   * @param allDeferredValues The deferred values to filter
   * @return A list of the deferred values that are of the given class
   * @param <T> The type of the {@link ValueAction} to filter out
   * @throws IllegalStateException if the values in allDeferredValues are not deferred, this comes from
   *              {@link NamedValue#valueAction} call
   */
  static <T extends ValueAction> List<T> filteredActions(Class<T> clazz,  List<NamedValue<?,?,?>> allDeferredValues) {
    if (allDeferredValues == null || allDeferredValues.isEmpty()) {
      return List.of();
    }

    return allDeferredValues.stream()
        .map(NamedValue::valueAction)
        .filter(clazz::isInstance)
        .map(clazz::cast)
        .toList();
  }
}
