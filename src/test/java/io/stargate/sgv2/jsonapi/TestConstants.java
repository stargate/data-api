package io.stargate.sgv2.jsonapi;

import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.EmbeddingSourceModel;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectIdentifier;
import io.stargate.sgv2.jsonapi.service.schema.SimilarityFunction;
import io.stargate.sgv2.jsonapi.service.schema.collections.*;

import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Re-usable values for tests.
 *
 * <p>This must be an instance so that quarkus can set up the environment, we need this because of
 * the use of their config library
 */
public class TestConstants {

  // Names
  public final String TEST_COMMAND_NAME = "testCommand";
  public final String KEYSPACE_NAME;
  public final String COLLECTION_NAME;
  public final SchemaObjectIdentifier SCHEMA_OBJECT_NAME;
  public final Tenant TENANT;

  // Schema objects for testing
  public final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT;
  public final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT_LEGACY;
  public final CollectionSchemaObject VECTOR_COLLECTION_SCHEMA_OBJECT;
  public final KeyspaceSchemaObject KEYSPACE_SCHEMA_OBJECT;
  public final DatabaseSchemaObject DATABASE_SCHEMA_OBJECT;

  public TestConstants() {

    TENANT = Tenant.create(DatabaseType.ASTRA, "test-tenant");
    KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
    COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
    SCHEMA_OBJECT_NAME = SchemaObjectIdentifier.forCollection(TENANT, KEYSPACE_NAME, COLLECTION_NAME);

    // Schema objects for testing
    COLLECTION_SCHEMA_OBJECT =
        new CollectionSchemaObject(
            SCHEMA_OBJECT_NAME.tenant(),
            SCHEMA_OBJECT_NAME.keyspace(),
            SCHEMA_OBJECT_NAME.table(),
            IdConfig.defaultIdConfig(),
            VectorConfig.NOT_ENABLED_CONFIG,
            null,
            CollectionLexicalConfig.configForDefault(),
            // Use default reranking config - hardcode the value to avoid reading config
            new CollectionRerankDef(
                true,
                new CollectionRerankDef.RerankServiceDef(
                    "nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", null, null)));

    // Schema object for testing with legacy (pre-lexical-config) defaults
    COLLECTION_SCHEMA_OBJECT_LEGACY =
        new CollectionSchemaObject(
            SCHEMA_OBJECT_NAME.tenant(),
            SCHEMA_OBJECT_NAME.keyspace(),
            SCHEMA_OBJECT_NAME.table(),
            IdConfig.defaultIdConfig(),
            VectorConfig.NOT_ENABLED_CONFIG,
            null,
            CollectionLexicalConfig.configForDisabled(),
            CollectionRerankDef.configForPreRerankingCollection());

    VECTOR_COLLECTION_SCHEMA_OBJECT =
        new CollectionSchemaObject(
            SCHEMA_OBJECT_NAME.tenant(),
            SCHEMA_OBJECT_NAME.keyspace(),
            SCHEMA_OBJECT_NAME.table(),
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
            CollectionLexicalConfig.configForDisabled(),
            CollectionRerankDef.configForPreRerankingCollection());

    KEYSPACE_SCHEMA_OBJECT = new KeyspaceSchemaObject(TENANT, KEYSPACE_NAME);
    DATABASE_SCHEMA_OBJECT = new DatabaseSchemaObject(TENANT);
  }

  // CommandContext for working on the schema objects above
  public CommandContext<CollectionSchemaObject> collectionContext() {
    return collectionContext(TEST_COMMAND_NAME, COLLECTION_SCHEMA_OBJECT, null, null);
  }

  public CommandContext<CollectionSchemaObject> collectionContext(
      String commandName,
      CollectionSchemaObject schema,
      JsonProcessingMetricsReporter metricsReporter,
      EmbeddingProvider embeddingProvider) {

    return CommandContext.builderSupplier()
        .withJsonProcessingMetricsReporter(
            metricsReporter == null ? mock(JsonProcessingMetricsReporter.class) : metricsReporter)
        .withCqlSessionCache(mock(CQLSessionCache.class))
        .withCommandConfig(new CommandConfig())
        .withEmbeddingProviderFactory(mock(EmbeddingProviderFactory.class))
        .withRerankingProviderFactory(mock(RerankingProviderFactory.class))
        .withMeterRegistry(mock(MeterRegistry.class))
        .getBuilder(schema)
        .withEmbeddingProvider(embeddingProvider)
        .withCommandName(commandName)
        .withRequestContext(new RequestContext(TENANT))
        .build();
  }

  public CommandContext<KeyspaceSchemaObject> keyspaceContext() {
    return keyspaceContext(
        TEST_COMMAND_NAME, KEYSPACE_SCHEMA_OBJECT, mock(JsonProcessingMetricsReporter.class));
  }

  public CommandContext<KeyspaceSchemaObject> keyspaceContext(
      String commandName,
      KeyspaceSchemaObject schema,
      JsonProcessingMetricsReporter metricsReporter) {

    return CommandContext.builderSupplier()
        .withJsonProcessingMetricsReporter(
            metricsReporter == null ? mock(JsonProcessingMetricsReporter.class) : metricsReporter)
        .withCqlSessionCache(mock(CQLSessionCache.class))
        .withCommandConfig(new CommandConfig())
        .withEmbeddingProviderFactory(mock(EmbeddingProviderFactory.class))
        .withRerankingProviderFactory(mock(RerankingProviderFactory.class))
        .withMeterRegistry(mock(MeterRegistry.class))
        .getBuilder(schema)
        .withCommandName(commandName)
        .withRequestContext(new RequestContext(TENANT))
        .build();
  }

  public CommandContext<DatabaseSchemaObject> databaseContext() {
    return CommandContext.builderSupplier()
        .withJsonProcessingMetricsReporter(mock(JsonProcessingMetricsReporter.class))
        .withCqlSessionCache(mock(CQLSessionCache.class))
        .withCommandConfig(new CommandConfig())
        .withEmbeddingProviderFactory(mock(EmbeddingProviderFactory.class))
        .withRerankingProviderFactory(mock(RerankingProviderFactory.class))
        .withMeterRegistry(mock(MeterRegistry.class))
        .getBuilder(DATABASE_SCHEMA_OBJECT)
        .withCommandName(TEST_COMMAND_NAME)
        .withRequestContext(new RequestContext(TENANT))
        .build();
  }

  public CollectionSchemaObject cloneWithIdConfig(
      CollectionSchemaObject original, CollectionIdType idType) {

    var idConfig = new IdConfig(idType);

    // because the TableMetadata was not used for the collections originally it is not required
    // tests often don't set it

    if (original.tableMetadata() == null) {
      return new CollectionSchemaObject(
          original.identifier().tenant(),
          original.identifier().keyspace(),
          original.identifier().table(),
          idConfig,
          original.vectorConfig(),
          original.indexingConfig(),
          original.lexicalConfig(),
          original.rerankingConfig());
    }

    return new CollectionSchemaObject(
        original.identifier().tenant(),
        original.tableMetadata(),
        idConfig,
        original.vectorConfig(),
        original.indexingConfig(),
        original.lexicalConfig(),
        original.rerankingConfig());
  }
}
