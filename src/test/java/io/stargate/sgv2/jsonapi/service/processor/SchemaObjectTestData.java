package io.stargate.sgv2.jsonapi.service.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject.SchemaObjectType;

/** Tests data and mocks for working with {@link SchemaObject} */
public class SchemaObjectTestData {

  public final String DATABASE_NAME = "database-" + System.currentTimeMillis();
  public final String KEYSPACE_NAME = "keyspace-" + System.currentTimeMillis();
  public final String COLLECTION_NAME = "collection-" + System.currentTimeMillis();
  public final String TABLE_NAME = "table-" + System.currentTimeMillis();

  public final DatabaseSchemaObject MOCK_DATABASE =
      mockSchemaObject(
          DatabaseSchemaObject.class, SchemaObjectType.DATABASE, SchemaObjectName.MISSING);
  public final KeyspaceSchemaObject MOCK_KEYSPACE =
      mockSchemaObject(
          KeyspaceSchemaObject.class,
          SchemaObjectType.KEYSPACE,
          new SchemaObjectName(KEYSPACE_NAME, SchemaObjectName.MISSING_NAME));
  public final CollectionSchemaObject MOCK_COLLECTION =
      mockSchemaObject(
          CollectionSchemaObject.class,
          SchemaObjectType.COLLECTION,
          new SchemaObjectName(KEYSPACE_NAME, COLLECTION_NAME));
  public final TableSchemaObject MOCK_TABLE =
      mockSchemaObject(
          TableSchemaObject.class,
          SchemaObjectType.TABLE,
          new SchemaObjectName(KEYSPACE_NAME, TABLE_NAME));

  /** helper to get the prebuilt mock instance on this class by the class of the schema object */
  @SuppressWarnings("unchecked")
  public <T extends SchemaObject> T prebuiltMock(Class<T> schemaType) {
    // cannot use new switch :(
    if (schemaType == DatabaseSchemaObject.class) {
      return (T) MOCK_DATABASE;
    } else if (schemaType == KeyspaceSchemaObject.class) {
      return (T) MOCK_KEYSPACE;
    } else if (schemaType == CollectionSchemaObject.class) {
      return (T) MOCK_COLLECTION;
    } else if (schemaType == TableSchemaObject.class) {
      return (T) MOCK_TABLE;
    } else {
      throw new IllegalArgumentException("Unknown schema object type: " + schemaType);
    }
  }

  /**
   * Create get mock and setup the type and name, used for internal. Call {@link
   * #prebuiltMock(Class)} to get the prebuilt mock on this instnace by the schema object class.
   */
  public <T extends SchemaObject> T mockSchemaObject(
      Class<T> schemaType, SchemaObjectType type, SchemaObjectName name) {

    T schema = mock(schemaType);
    when(schema.type()).thenReturn(type);
    when(schema.name()).thenReturn(name);
    return schema;
  }
}
