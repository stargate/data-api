package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

public class TableSchemaObject extends SchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.TABLE;

  /** Represents missing schema, e.g. when we are running a create table. */
  public static final TableSchemaObject MISSING = new TableSchemaObject(SchemaObjectName.MISSING);

  // TODO: hold the table meta data, need to work out how we handle mock tables in test etc.
  //  public final TableMetadata tableMetadata;

  public TableSchemaObject(String keyspace, String name) {
    this(new SchemaObjectName(keyspace, name));
  }

  public TableSchemaObject(SchemaObjectName name) {
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
