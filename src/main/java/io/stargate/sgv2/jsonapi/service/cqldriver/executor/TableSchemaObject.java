package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiTableDef;

public class TableSchemaObject extends TableBasedSchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  private final VectorConfig vectorConfig;
  private final ApiTableDef apiTableDef;

  private TableSchemaObject(
      TableMetadata tableMetadata, VectorConfig vectorConfig, ApiTableDef apiTableDef) {
    super(TYPE, tableMetadata);
    this.vectorConfig = vectorConfig;
    this.apiTableDef = apiTableDef;
  }

  @Override
  public VectorConfig vectorConfig() {
    return vectorConfig;
  }

  @Override
  public IndexUsage newIndexUsage() {
    return IndexUsage.NO_OP;
  }

  public ApiTableDef apiTableDef() {
    return apiTableDef;
  }

  /** Get table schema object from table metadata */
  public static TableSchemaObject from(TableMetadata tableMetadata, ObjectMapper objectMapper) {

    var vectorConfig = VectorConfig.from(tableMetadata, objectMapper);
    var apiTableDef = ApiTableDef.FROM_CQL_FACTORY.create(tableMetadata, vectorConfig);
    return new TableSchemaObject(tableMetadata, vectorConfig, apiTableDef);
  }
}
