package io.stargate.sgv2.jsonapi;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObjectName;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Re-usable values for tests TODO: move this from static to instance so that the keyspace and
 * collections names get generated for each test
 */
public final class TestConstants {

  // Random keyspace and collection names, NOTE: these are static for all tests
  public static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  public static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  public static final SchemaObjectName SCHEMA_OBJECT_NAME =
      new SchemaObjectName(KEYSPACE_NAME, COLLECTION_NAME);

  // A Collection and Keyspace with the name from above.
  public static final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT =
      new CollectionSchemaObject(
          SCHEMA_OBJECT_NAME,
          CollectionSchemaObject.IdConfig.defaultIdConfig(),
          VectorConfig.notEnabledVectorConfig(),
          null);

  public static final KeyspaceSchemaObject KEYSPACE_SCHEMA_OBJECT =
      KeyspaceSchemaObject.fromSchemaObject(COLLECTION_SCHEMA_OBJECT);

  public static final String COMMAND_NAME = "testCommand";

  // CommandContext for working on the Collection or the Keyspace
  public static final CommandContext<CollectionSchemaObject> COLLECTION_CONTEXT =
      new CommandContext<>(COLLECTION_SCHEMA_OBJECT, null, COMMAND_NAME, null);

  public static final CommandContext<KeyspaceSchemaObject> KEYSPACE_CONTEXT =
      new CommandContext<>(KEYSPACE_SCHEMA_OBJECT, null, COMMAND_NAME, null);
}
