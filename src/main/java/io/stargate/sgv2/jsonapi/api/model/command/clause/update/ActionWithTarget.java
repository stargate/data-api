package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

/**
 * Interface needed to allow easy sorting by {@code path} property exposed by Action record types.
 */
public interface ActionWithTarget {
  /** @return Path that the action targets (dotted notation) */
  ActionTargetLocator target();

  /** Convenience method: path from {@code action()} */
  default String path() {
    return target().path();
  }
}
