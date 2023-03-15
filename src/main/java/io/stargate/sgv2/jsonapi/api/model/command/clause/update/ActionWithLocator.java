package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import io.stargate.sgv2.jsonapi.util.PathMatchLocator;

/**
 * Interface needed to allow easy sorting by {@code path} property exposed by Action record types.
 */
public interface ActionWithLocator {
  /** @return Path that the action targets (dotted notation) */
  PathMatchLocator locator();

  /** Convenience method: path from {@code action()} */
  default String path() {
    return locator().path();
  }
}
