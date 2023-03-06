package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

/**
 * Interface needed to allow easy sorting by {@code path} property exposed by Action record types.
 */
public interface ActionWithPath {
  /** @return Path that the action targets (dotted notation) */
  String path();
}
