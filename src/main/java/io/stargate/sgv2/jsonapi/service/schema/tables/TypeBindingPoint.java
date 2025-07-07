package io.stargate.sgv2.jsonapi.service.schema.tables;

/**
 * The location a type is used in the schema of a table.
 *
 * <p>Often used with {@link BindingPointRules}.
 */
public enum TypeBindingPoint {
  /** Type is used to define the value of a list, map, or set. */
  COLLECTION_VALUE,
  /** Type is used to define the key of a map. */
  MAP_KEY,
  /** Type is used to define a column in a table. */
  TABLE_COLUMN,
  /** Type is used to define a field in a user defined type */
  UDT_FIELD;
}
