package io.stargate.sgv2.jsonapi;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObjectName;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorConfig;
import org.apache.commons.lang3.RandomStringUtils;

/** Re-usable values for tests */
public final class TestConstants {

  public static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  public static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  public static final SchemaObjectName SCHEMA_OBJECT_NAME =
      new SchemaObjectName(KEYSPACE_NAME, COLLECTION_NAME);

  public static final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT =
      new CollectionSchemaObject(
          SCHEMA_OBJECT_NAME,
          CollectionSchemaObject.IdConfig.defaultIdConfig(),
          VectorConfig.notEnabledVectorConfig(),
          null);

  public static final String COMMAND_NAME = "testCommand";

  public static final CommandContext<CollectionSchemaObject> CONTEXT =
      new CommandContext<>(COLLECTION_SCHEMA_OBJECT, null, COMMAND_NAME, null);
}
