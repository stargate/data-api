package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

public class TableSchemaObject extends TableBasedSchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  public TableSchemaObject(TableMetadata tableMetadata) {
    super(TYPE, tableMetadata);
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
