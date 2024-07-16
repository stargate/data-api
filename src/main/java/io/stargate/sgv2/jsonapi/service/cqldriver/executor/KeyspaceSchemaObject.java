package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

public class KeyspaceSchemaObject extends SchemaObject {

  public static final SchemaObjectType TYPE = SchemaObjectType.KEYSPACE;

  /** Represents missing schema, e.g. when we are running a create table. */
  public static final KeyspaceSchemaObject MISSING =
      new KeyspaceSchemaObject(SchemaObjectName.MISSING);

  public KeyspaceSchemaObject(String keyspace) {
    this(newObjectName(keyspace));
  }

  public KeyspaceSchemaObject(SchemaObjectName name) {
    super(TYPE, name);
  }

  /**
   * Construct a {@link KeyspaceSchemaObject} that represents the keyspace the collection is in.
   *
   * @param collection
   * @return
   */
  public static KeyspaceSchemaObject fromSchemaObject(CollectionSchemaObject collection) {
    return new KeyspaceSchemaObject(newObjectName(collection.name.keyspace()));
  }

  /**
   * Construct a {@link KeyspaceSchemaObject} that represents the keyspace the collection is in.
   *
   * @param table
   * @return
   */
  public static KeyspaceSchemaObject fromSchemaObject(TableSchemaObject table) {
    return new KeyspaceSchemaObject(newObjectName(table.name.keyspace()));
  }

  @Override
  public VectorConfig vectorConfig() {
    return VectorConfig.notEnabledVectorConfig();
  }

  @Override
  public IndexUsage newIndexUsage() {
    return IndexUsage.NO_OP;
  }

  /**
   * Centralised creation of the name for a Keyspace so we always use the correct marker object for
   * collection name
   *
   * @param keyspaceName
   * @return
   */
  private static SchemaObjectName newObjectName(String keyspaceName) {
    return new SchemaObjectName(keyspaceName, SchemaObjectName.MISSING_NAME);
  }
}
