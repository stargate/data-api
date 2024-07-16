package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

public class KeyspaceSchemaObject extends SchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.KEYSPACE;

  /** Represents missing schema, e.g. when we are running a create table. */
  public static final KeyspaceSchemaObject MISSING =
      new KeyspaceSchemaObject(SchemaObjectName.MISSING);

  public KeyspaceSchemaObject(String keyspace) {
    this(new SchemaObjectName(keyspace, SchemaObjectName.MISSING_NAME));
  }

  public KeyspaceSchemaObject(SchemaObjectName name) {
    super(TYPE, name);
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
