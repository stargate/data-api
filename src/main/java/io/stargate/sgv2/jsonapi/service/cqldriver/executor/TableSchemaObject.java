package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

public class TableSchemaObject extends SchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  /** Represents missing schema, e.g. when we are running a create table. */
  // public static final TableSchemaObject MISSING = new
  // TableSchemaObject(SchemaObjectName.MISSING);

  public final TableMetadata tableMetadata;

  // TODO: hold the table meta data, need to work out how we handle mock tables in test etc.
  //  public final TableMetadata tableMetadata;

  public TableSchemaObject(TableMetadata tableMetadata) {
    super(
        TYPE,
        new SchemaObjectName(
            tableMetadata.getKeyspace().asCql(false), tableMetadata.getName().asCql(false)));
    this.tableMetadata = tableMetadata;
  }

  @Override
  public VectorConfig vectorConfig() {
    return VectorConfig.notEnabledVectorConfig();
  }

  @Override
  public IndexUsage newIndexUsage() {
    return IndexUsage.NO_OP;
  }
}
