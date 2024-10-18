package io.stargate.sgv2.jsonapi.service.operation.tables;

/**
 * Represents an attempt to alter a table schema. The attempt can be of 3 types adding columns,
 * dropping columns, or updating extensions.
 */
public enum AlterTableType {
  ADD_COLUMNS,
  DROP_COLUMNS,
  UPDATE_EXTENSIONS
}
