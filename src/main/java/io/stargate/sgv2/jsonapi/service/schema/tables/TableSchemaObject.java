package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.IndexUsage;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectType;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableSchemaObject extends TableBasedSchemaObject {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSchemaObject.class);

  private final VectorConfig vectorConfig;
  private final ApiTableDef apiTableDef;

  private TableSchemaObject(
      Tenant tenant,
      TableMetadata tableMetadata,
      VectorConfig vectorConfig,
      ApiTableDef apiTableDef) {
    super(SchemaObjectType.TABLE, tenant, tableMetadata);
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
  public static TableSchemaObject from(
      Tenant tenant, TableMetadata tableMetadata, ObjectMapper objectMapper) {

    var vectorConfig = VectorConfig.from(tableMetadata, objectMapper);
    var apiTableDef = ApiTableDef.FROM_CQL_FACTORY.create(tableMetadata, vectorConfig);
    return new TableSchemaObject(tenant, tableMetadata, vectorConfig, apiTableDef);
  }

  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder).append("apiTableDef", apiTableDef);
  }
}
