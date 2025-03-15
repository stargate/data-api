package io.stargate.sgv2.jsonapi;

import static org.mockito.Mockito.mock;

import io.stargate.sgv2.jsonapi.api.model.command.CommandConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import java.util.List;
import java.util.Optional;
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

  // Schema objects for testing: uses current ("new") defaults
  public static final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT =
      new CollectionSchemaObject(
          SCHEMA_OBJECT_NAME,
          null,
          IdConfig.defaultIdConfig(),
          VectorConfig.NOT_ENABLED_CONFIG,
          null,
          // Use configs that by default enable lexical:
          CollectionLexicalConfig.configForNewCollections(),
          // Use default rerank config - hardcode the value to avoid reading config
          new CollectionRerankConfig(
              true,
              new CollectionRerankConfig.RerankingProviderConfig(
                  "nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", null, null)));

  // Schema object for testing with legacy (pre-lexical-config) defaults
  public static final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT_LEGACY =
      new CollectionSchemaObject(
          SCHEMA_OBJECT_NAME,
          null,
          IdConfig.defaultIdConfig(),
          VectorConfig.NOT_ENABLED_CONFIG,
          null,
          CollectionLexicalConfig.configForLegacyCollections(),
          CollectionRerankConfig.configForLegacyCollections());

  public static final CollectionSchemaObject VECTOR_COLLECTION_SCHEMA_OBJECT =
      new CollectionSchemaObject(
          SCHEMA_OBJECT_NAME,
          null,
          IdConfig.defaultIdConfig(),
          VectorConfig.fromColumnDefinitions(
              List.of(
                  new VectorColumnDefinition(
                      DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD,
                      -1,
                      SimilarityFunction.COSINE,
                      EmbeddingSourceModel.OTHER,
                      null))),
          null,
          CollectionLexicalConfig.configForLegacyCollections(),
          CollectionRerankConfig.configForLegacyCollections());

  public static final KeyspaceSchemaObject KEYSPACE_SCHEMA_OBJECT =
      KeyspaceSchemaObject.fromSchemaObject(COLLECTION_SCHEMA_OBJECT);

  public static final DatabaseSchemaObject DATABASE_SCHEMA_OBJECT = new DatabaseSchemaObject();

  public static final String TEST_COMMAND_NAME = "testCommand";

  // CommandContext for working on the schema objects above

  public static final ApiFeatures DEFAULT_API_FEATURES_FOR_TESTS = ApiFeatures.empty();

  public static CommandContext<CollectionSchemaObject> collectionContext() {
    return collectionContext(TEST_COMMAND_NAME, COLLECTION_SCHEMA_OBJECT, null, null);
  }

  //  public static CommandContext<CollectionSchemaObject> collectionContext(
  //      String commandName,
  //      CollectionSchemaObject schema,
  //      JsonProcessingMetricsReporter metricsReporter) {
  //    return collectionContext(commandName, schema, metricsReporter, null);
  //  }

  public static CommandContext<CollectionSchemaObject> collectionContext(
      String commandName,
      CollectionSchemaObject schema,
      JsonProcessingMetricsReporter metricsReporter,
      EmbeddingProvider embeddingProvider) {

    return CommandContext.builderSupplier()
        .withJsonProcessingMetricsReporter(
            metricsReporter == null ? mock(JsonProcessingMetricsReporter.class) : metricsReporter)
        .withCqlSessionCache(mock(CQLSessionCache.class))
        .withCommandConfig(new CommandConfig())
        .getBuilder(schema)
        .withEmbeddingProvider(embeddingProvider)
        .withCommandName(commandName)
        .withRequestContext(new RequestContext(Optional.of("test-tenant")))
        .build();
  }

  public static CommandContext<KeyspaceSchemaObject> keyspaceContext() {
    return keyspaceContext(
        TEST_COMMAND_NAME, KEYSPACE_SCHEMA_OBJECT, mock(JsonProcessingMetricsReporter.class));
  }

  public static CommandContext<KeyspaceSchemaObject> keyspaceContext(
      String commandName,
      KeyspaceSchemaObject schema,
      JsonProcessingMetricsReporter metricsReporter) {

    return CommandContext.builderSupplier()
        .withJsonProcessingMetricsReporter(
            metricsReporter == null ? mock(JsonProcessingMetricsReporter.class) : metricsReporter)
        .withCqlSessionCache(mock(CQLSessionCache.class))
        .withCommandConfig(new CommandConfig())
        .getBuilder(schema)
        .withCommandName(commandName)
        .withRequestContext(new RequestContext(Optional.of("test-tenant")))
        .build();
  }

  private static final CommandContext<DatabaseSchemaObject> DATABASE_CONTEXT =
      CommandContext.builderSupplier()
          .withJsonProcessingMetricsReporter(mock(JsonProcessingMetricsReporter.class))
          .withCqlSessionCache(mock(CQLSessionCache.class))
          .withCommandConfig(new CommandConfig())
          .getBuilder(DATABASE_SCHEMA_OBJECT)
          .withCommandName(TEST_COMMAND_NAME)
          .withRequestContext(new RequestContext(Optional.of("test-tenant")))
          .build();

  public static CommandContext<DatabaseSchemaObject> databaseContext() {
    return DATABASE_CONTEXT;
  }
}
