package io.stargate.sgv2.jsonapi;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import java.util.List;
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

  // Schema objects for testing
  public static final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT =
      new CollectionSchemaObject(
          SCHEMA_OBJECT_NAME,
          null,
          IdConfig.defaultIdConfig(),
          VectorConfig.NOT_ENABLED_CONFIG,
          null);

  public static final CollectionSchemaObject VECTOR_COLLECTION_SCHEMA_OBJECT =
      new CollectionSchemaObject(
          SCHEMA_OBJECT_NAME,
          null,
          IdConfig.defaultIdConfig(),
          new VectorConfig(
              List.of(
                  new VectorConfig.ColumnVectorDefinition(
                      DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                      -1,
                      SimilarityFunction.COSINE,
                      null))),
          null);

  public static final KeyspaceSchemaObject KEYSPACE_SCHEMA_OBJECT =
      KeyspaceSchemaObject.fromSchemaObject(COLLECTION_SCHEMA_OBJECT);

  public static final DatabaseSchemaObject DATABASE_SCHEMA_OBJECT = new DatabaseSchemaObject();

  public static final String TEST_COMMAND_NAME = "testCommand";

  // CommandContext for working on the schema objects above

  public static final ApiFeatures DEFAULT_API_FEATURES_FOR_TESTS = ApiFeatures.empty();

  public static final CommandContext<CollectionSchemaObject> COLLECTION_CONTEXT =
      new CommandContext<>(
          COLLECTION_SCHEMA_OBJECT, null, TEST_COMMAND_NAME, null, DEFAULT_API_FEATURES_FOR_TESTS);

  public static final CommandContext<CollectionSchemaObject> VECTOR_COLLECTION_CONTEXT =
      new CommandContext<>(
          VECTOR_COLLECTION_SCHEMA_OBJECT, null, null, null, DEFAULT_API_FEATURES_FOR_TESTS);

  public static final CommandContext<KeyspaceSchemaObject> KEYSPACE_CONTEXT =
      new CommandContext<>(
          KEYSPACE_SCHEMA_OBJECT, null, TEST_COMMAND_NAME, null, DEFAULT_API_FEATURES_FOR_TESTS);

  public static final CommandContext<DatabaseSchemaObject> DATABASE_CONTEXT =
      new CommandContext<>(
          DATABASE_SCHEMA_OBJECT, null, TEST_COMMAND_NAME, null, DEFAULT_API_FEATURES_FOR_TESTS);
}
