package io.stargate.sgv2.jsonapi;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.CommandConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentials;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.UserAgent;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.api.request.tenant.TenantFactory;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.*;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalConfig;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.IdConfig;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil;
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

  public final String APP_NAME;
  public final DatabaseType DATABASE_TYPE;
  public final TenantFactory SINGLETON_TENANT_FACTORY;

  // ============================================================
  // Names
  // ============================================================

  /**
   * A unique identifier for the test run, append to names to ensure uniqueness and stable ID for
   * the test class instance.
   */
  public final String CORRELATION_ID;

  public final String COMMAND_NAME;
  public final String KEYSPACE_NAME;
  public final String COLLECTION_NAME;
  public final String TABLE_NAME;

  /** Raw SLA user agent, Use {@link #SLA_USER_AGENT} */
  public final String SLA_USER_AGENT_NAME = "Datastax-SLA-Checker";

  // ============================================================
  // Request Context
  // ============================================================

  /** An astra database type TENANT used for test */
  public final Tenant TENANT;

  /** A database identifier for the test */
  public final SchemaObjectIdentifier DATABASE_IDENTIFIER;

  /** A keyspace identifier for the test */
  public final SchemaObjectIdentifier KEYSPACE_IDENTIFIER;

  /** A collection identifier for the test */
  public final SchemaObjectIdentifier COLLECTION_IDENTIFIER;

  /** A table identifier for the test */
  public final SchemaObjectIdentifier TABLE_IDENTIFIER;

  /** A cassandra database type TENANT used for test */
  public final Tenant CASSANDRA_TENANT;

  public final String AUTH_TOKEN;

  /** A non SLA user agent for the test run */
  public final UserAgent USER_AGENT;

  /** the DS SLA USer Agent */
  public final UserAgent SLA_USER_AGENT;

  /** Embedding credentials */
  public final EmbeddingCredentials EMBEDDING_CREDENTIALS;

  /**
   * Collection Schema to use if all information missing: Vector not configured, no Lexical enabled
   */
  public final CollectionSchemaObject MISSING;

  // ============================================================
  // Schema Objects
  // ============================================================

  public final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT;
  public final CollectionSchemaObject COLLECTION_SCHEMA_OBJECT_LEGACY;
  public final CollectionSchemaObject VECTOR_COLLECTION_SCHEMA_OBJECT;
  public final TableSchemaObject TABLE_SCHEMA_OBJECT;
  public final KeyspaceSchemaObject KEYSPACE_SCHEMA_OBJECT;
  public final DatabaseSchemaObject DATABASE_SCHEMA_OBJECT;

  public TestConstants() {
    // ============================================================
    // Names
    // ============================================================
    CORRELATION_ID = "test-id-" + RandomStringUtils.insecure().nextAlphanumeric(16);

    COMMAND_NAME = "command-" + CORRELATION_ID;
    KEYSPACE_NAME = "keyspace-" + CORRELATION_ID;
    var keyspaceCqlIdentifier = CqlIdentifierUtil.cqlIdentifierFromUserInput(KEYSPACE_NAME);
    COLLECTION_NAME = "collection-" + CORRELATION_ID;
    var collectionCqlIdentifier = CqlIdentifierUtil.cqlIdentifierFromUserInput(COLLECTION_NAME);
    TABLE_NAME = "table-" + CORRELATION_ID;

    APP_NAME = "Stargate DATA API -" + CORRELATION_ID;

    // ============================================================
    // Request Context
    // ============================================================

    DATABASE_TYPE = DatabaseType.ASTRA;
    var tenantId = "tenant-" + CORRELATION_ID;
    TenantFactory.reset();
    TenantFactory.initialize(DATABASE_TYPE);
    TENANT = TenantFactory.instance().create(tenantId);

    SINGLETON_TENANT_FACTORY = TenantFactory.instance();

    // will be defaulted to SINGLE-TENANT, so passing null as tenantId
    CASSANDRA_TENANT = Tenant.create(DatabaseType.CASSANDRA, null);

    AUTH_TOKEN = "auth-token-" + CORRELATION_ID;

    var userAgentString = "user-agent/" + CORRELATION_ID;
    USER_AGENT = new UserAgent(userAgentString);

    var slaUserAgentString = SLA_USER_AGENT_NAME + "/" + CORRELATION_ID;
    SLA_USER_AGENT = new UserAgent(slaUserAgentString);

    EMBEDDING_CREDENTIALS =
        new EmbeddingCredentials(
            TENANT, Optional.of("test-api-key"), Optional.empty(), Optional.empty());

    // ============================================================
    // Schema Objects
    // ============================================================

    DATABASE_IDENTIFIER = SchemaObjectIdentifier.forDatabase(TENANT);
    KEYSPACE_IDENTIFIER = SchemaObjectIdentifier.forKeyspace(TENANT, keyspaceCqlIdentifier);
    COLLECTION_IDENTIFIER =
        SchemaObjectIdentifier.forCollection(
            TENANT, keyspaceCqlIdentifier, collectionCqlIdentifier);
    TABLE_IDENTIFIER =
        SchemaObjectIdentifier.forTable(
            TENANT,
            keyspaceCqlIdentifier,
            CqlIdentifierUtil.cqlIdentifierFromUserInput(TABLE_NAME));

    COLLECTION_SCHEMA_OBJECT =
        new CollectionSchemaObject(
            COLLECTION_IDENTIFIER,
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
            COLLECTION_IDENTIFIER,
            IdConfig.defaultIdConfig(),
            VectorConfig.NOT_ENABLED_CONFIG,
            null,
            CollectionLexicalConfig.configForDisabled(),
            CollectionRerankDef.configForPreRerankingCollection());

    VECTOR_COLLECTION_SCHEMA_OBJECT =
        new CollectionSchemaObject(
            COLLECTION_IDENTIFIER,
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

    TABLE_SCHEMA_OBJECT = new TableSchemaObject(TABLE_IDENTIFIER);

    KEYSPACE_SCHEMA_OBJECT = new KeyspaceSchemaObject(KEYSPACE_IDENTIFIER);
    DATABASE_SCHEMA_OBJECT = new DatabaseSchemaObject(TENANT);

    MISSING =
        new CollectionSchemaObject(
            TENANT,
            null,
            IdConfig.defaultIdConfig(),
            VectorConfig.NOT_ENABLED_CONFIG,
            null,
            CollectionLexicalConfig.configForDisabled(),
            CollectionRerankDef.configForDisabled());
  }

  // CommandContext for working on the schema objects above
  public CommandContext<CollectionSchemaObject> collectionContext() {
    return collectionContext(COMMAND_NAME, COLLECTION_SCHEMA_OBJECT, null, null);
  }

  public CommandContext<CollectionSchemaObject> collectionContext(
      String commandName,
      CollectionSchemaObject schema,
      JsonProcessingMetricsReporter metricsReporter,
      EmbeddingProvider embeddingProvider) {

    var requestContext = mock(RequestContext.class);
    when(requestContext.tenant()).thenReturn(TENANT);
    when(requestContext.getEmbeddingCredentials()).thenReturn(EMBEDDING_CREDENTIALS);

    var embeddingCredentials = mock(EmbeddingCredentials.class);
    when(embeddingCredentials.tenant()).thenReturn(TENANT);
    when(embeddingCredentials.apiKey()).thenReturn(Optional.of("test-apiKey"));
    when(embeddingCredentials.accessId()).thenReturn(Optional.of("test-accessId"));
    when(embeddingCredentials.secretId()).thenReturn(Optional.of("test-secretId"));

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
        .withRequestContext(requestContext)
        .build();
  }

  public CommandContext<KeyspaceSchemaObject> keyspaceContext() {
    return keyspaceContext(
        COMMAND_NAME, KEYSPACE_SCHEMA_OBJECT, mock(JsonProcessingMetricsReporter.class));
  }

  public RequestContext requestContext() {
    return new RequestContext(TENANT, AUTH_TOKEN, USER_AGENT);
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
        .withRequestContext(requestContext())
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
        .withCommandName(COMMAND_NAME)
        .withRequestContext(requestContext())
        .build();
  }
}
