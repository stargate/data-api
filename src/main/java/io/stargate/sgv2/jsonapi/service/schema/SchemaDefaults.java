package io.stargate.sgv2.jsonapi.service.schema;

/**
 * For use with the {@link SchemaFactory} this interface provides a standard way to talk about the
 * different contexts in which we talk about a schema config.
 *
 * @param <T> Type of the schema being created.
 */
public interface SchemaDefaults<T> {
  /**
   * Called to get the value of this schema config to use for schema created before the feature was
   * released.
   */
  T forPreRelease();

  /** Called to get the value of this schema config to use for the current default. */
  T currentDefault();

  /**
   * Called to get the value of this schema config, for after the feature was released but when the
   * feature is disabled.
   *
   * <p>e.g. when an index capability is released but not all environments support it.
   */
  T forDisabledFeature();
}
