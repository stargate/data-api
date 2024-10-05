package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import java.util.List;

public class DatabaseSchemaObject extends SchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.DATABASE;

  public DatabaseSchemaObject() {
    super(TYPE, SchemaObjectName.MISSING);
  }

  @Override
  public List<VectorConfig> vectorConfigs() {
    return List.of(VectorConfig.notEnabledVectorConfig());
  }

  @Override
  public IndexUsage newIndexUsage() {
    return IndexUsage.NO_OP;
  }
}
