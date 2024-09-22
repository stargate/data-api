package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

public abstract class TableBasedSchemaObject extends SchemaObject {

  private final TableMetadata tableMetadata;

  protected TableBasedSchemaObject(SchemaObjectType type, TableMetadata tableMetadata) {
    // uses asCql(pretty) so the names do not always include double quotes
    this(
        type,
        tableMetadata == null
            ? SchemaObjectName.MISSING
            : new SchemaObjectName(
                tableMetadata.getKeyspace().asCql(true), tableMetadata.getName().asCql(true)),
        tableMetadata);
  }

  // aaron- adding this ctor so for now the CollectionSchemaObject can set the schemaObjectName and
  // have the tablemetdata
  // be null because it is not used by any collection processing (currently).
  protected TableBasedSchemaObject(
      SchemaObjectType type, SchemaObjectName name, TableMetadata tableMetadata) {
    // uses asCql(pretty) so the names do not always include double quotes
    super(type, name);
    this.tableMetadata = tableMetadata;
  }

  public TableMetadata tableMetadata() {
    return tableMetadata;
  }
}
