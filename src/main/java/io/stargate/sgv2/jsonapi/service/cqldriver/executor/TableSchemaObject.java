package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

public class TableSchemaObject extends SchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  public final TableMetadata tableMetadata;

  public TableSchemaObject(TableMetadata tableMetadata) {
    // uses asCwl(pretty) so the names do not always include double quotes
    super(
        TYPE,
        new SchemaObjectName(
            tableMetadata.getKeyspace().asCql(true), tableMetadata.getName().asCql(true)));
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
