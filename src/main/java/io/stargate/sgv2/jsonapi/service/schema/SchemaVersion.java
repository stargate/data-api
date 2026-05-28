package io.stargate.sgv2.jsonapi.service.schema;

/** A base interface so we can have different schema versions for tables and collections */
public interface SchemaVersion {

  /**
   * Integer value used to both order versions, in ascending order, and to store the value for this
   * version in JSON. There is no "semantic versioning" for the schema, it is a simple higher
   * integer is more recent.
   *
   * <p>Implementations should also return this value for `toString()`
   *
   * @return Integer value that may be less than zero, used to order versions where higher values
   *     are more recent.
   */
  int ordinalValue();
}
