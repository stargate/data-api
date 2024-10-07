package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

public class DatabaseSchemaObject extends SchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.DATABASE;

  public DatabaseSchemaObject() {
    super(TYPE, SchemaObjectName.MISSING);
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
