package io.stargate.sgv2.jsonapi.api.model.command.table;

import java.util.Optional;

/**
 * Describes where the schema description will be used when building a response to a user.
 */
public enum SchemaDescBindingPoint {

  /**
   * Schema description used as the top-level response of a DDL command,
   * such as listTables or describeTable.
   */
  DDL_SCHEMA_OBJECT,


  /**
   * Schema description used within {@link #DDL_SCHEMA_OBJECT}
   * as a reference to a schema object, such as a column in a table.
   */
  DDL_USAGE,

  /**
   * Schema description used as the top-level response of a DML command,
   * as part of the inline schema for a read or write operation.
   */
  DML_SCHEMA_OBJECT,

  /**
   * Schema description used within {@link #DML_SCHEMA_OBJECT}
   * as a reference to a schema object, such as a column in a table.
   */
  DML_USAGE
  ;

  /**
   * Helper to create a new {@link UnsupportedOperationException} with a message for when
   * SchemaDescBindingPoint is not supported in a given context.
   * @param context e.g. "ApiTableDef.getSchemaDescription()"
   * @return a new UnsupportedOperationException with a message, throw it yourself :)
   */
  public UnsupportedOperationException unsupportedException(String context) {
    return new UnsupportedOperationException(
        context + " - unsupported SchemaDescBindingPoint: " + this);
  }
}
