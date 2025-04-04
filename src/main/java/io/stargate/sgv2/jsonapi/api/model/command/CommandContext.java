package io.stargate.sgv2.jsonapi.api.model.command;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.DefaultRequestTracing;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.Objects;

/**
 * Defines the context in which to execute the command, providing access to the schema and config,
 * and any external resources for the command to use.
 *
 * <p>To get an instance, call {@link CommandContext#builderSupplier()} to get a {@link
 * BuilderSupplier}, configure this with application wide config, then when ready to build the
 * context for a specific request call {@link BuilderSupplier#getBuilder(SchemaObject)} to get a
 * {@link BuilderSupplier.Builder} to configure the context for the request.
 *
 * <p>
 *
 * @param <SchemaT> The schema object type that this context is for. There are times we need to lock
 *     this down to the specific type, if so use the "as" methods such as {@link
 *     CommandContext#asCollectionContext()}
 */
public class CommandContext<SchemaT extends SchemaObject> {

  // Common for all instances
  private final JsonProcessingMetricsReporter jsonProcessingMetricsReporter;
  private final CQLSessionCache cqlSessionCache;
  private final CommandConfig commandConfig;
  private final EmbeddingProviderFactory embeddingProviderFactory;
  private final RerankingProviderFactory rerankingProviderFactory;
  private final MeterRegistry meterRegistry;

  // Request specific
  private final SchemaT schemaObject;
  private final EmbeddingProvider
      embeddingProvider; // to be removed later, this is a single provider
  private final String commandName; // TODO: remove the command name, but it is used in 14 places
  private final RequestContext requestContext;
  private RequestTracing requestTracing;

  // created on demand or set via builder, otherwise we need to read from config too early when
  // running tests, See the {@link Builder#withApiFeatures}
  // access via {@link CommandContext#apiFeatures()}
  private ApiFeatures apiFeatures;

  private CommandContext(
      SchemaT schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      RequestContext requestContext,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      CQLSessionCache cqlSessionCache,
      CommandConfig commandConfig,
      ApiFeatures apiFeatures,
      EmbeddingProviderFactory embeddingProviderFactory,
      RerankingProviderFactory rerankingProviderFactory,
      MeterRegistry meterRegistry) {

    this.schemaObject = schemaObject;
    this.embeddingProvider = embeddingProvider;
    this.commandName = commandName;
    this.requestContext = requestContext;

    this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
    this.cqlSessionCache = cqlSessionCache;
    this.commandConfig = commandConfig;
    this.embeddingProviderFactory = embeddingProviderFactory;
    this.rerankingProviderFactory = rerankingProviderFactory;

    this.apiFeatures = apiFeatures;
    this.meterRegistry = meterRegistry;

    var anyTracing =
        apiFeatures().isFeatureEnabled(ApiFeature.REQUEST_TRACING)
            || apiFeatures().isFeatureEnabled(ApiFeature.REQUEST_TRACING_FULL);

    this.requestTracing =
        anyTracing
            ? new DefaultRequestTracing(
                requestContext.getRequestId(),
                requestContext.getTenantId().orElse(""),
                apiFeatures().isFeatureEnabled(ApiFeature.REQUEST_TRACING_FULL))
            : RequestTracing.NO_OP;
  }

  /** See doc comments for {@link CommandContext} */
  public static BuilderSupplier builderSupplier() {
    return new BuilderSupplier();
  }

  public SchemaT schemaObject() {
    return schemaObject;
  }

  public EmbeddingProvider embeddingProvider() {
    return embeddingProvider;
  }

  public RerankingProviderFactory rerankingProviderFactory() {
    return rerankingProviderFactory;
  }

  public String commandName() {
    return commandName;
  }

  public RequestTracing requestTracing() {
    return requestTracing;
  }

  public RequestContext requestContext() {
    return requestContext;
  }

  public ApiFeatures apiFeatures() {
    // using a sync block here because the context can be accessed by multiple tasks concurrently
    if (apiFeatures == null) {
      synchronized (this) {
        if (apiFeatures == null) {
          // Merging the config for features with the request headers to get the final feature set
          apiFeatures =
              ApiFeatures.fromConfigAndRequest(
                  commandConfig.get(FeaturesConfig.class), requestContext.getHttpHeaders());
        }
      }
    }
    return apiFeatures;
  }

  public JsonProcessingMetricsReporter jsonProcessingMetricsReporter() {
    return jsonProcessingMetricsReporter;
  }

  public CQLSessionCache cqlSessionCache() {
    return cqlSessionCache;
  }

  public CommandConfig config() {
    return commandConfig;
  }

  public EmbeddingProviderFactory embeddingProviderFactory() {
    return embeddingProviderFactory;
  }

  public MeterRegistry meterRegistry() {
    return meterRegistry;
  }

  public boolean isCollectionContext() {
    return schemaObject().type() == CollectionSchemaObject.TYPE;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<CollectionSchemaObject> asCollectionContext() {
    checkSchemaObjectType(CollectionSchemaObject.TYPE);
    return (CommandContext<CollectionSchemaObject>) this;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<TableSchemaObject> asTableContext() {
    checkSchemaObjectType(TableSchemaObject.TYPE);
    return (CommandContext<TableSchemaObject>) this;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<KeyspaceSchemaObject> asKeyspaceContext() {
    checkSchemaObjectType(KeyspaceSchemaObject.TYPE);
    return (CommandContext<KeyspaceSchemaObject>) this;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<DatabaseSchemaObject> asDatabaseContext() {
    checkSchemaObjectType(DatabaseSchemaObject.TYPE);
    return (CommandContext<DatabaseSchemaObject>) this;
  }

  private void checkSchemaObjectType(SchemaObject.SchemaObjectType expectedType) {
    Preconditions.checkArgument(
        schemaObject().type() == expectedType,
        "SchemaObject type actual was %s expected was %s ",
        schemaObject().type(),
        expectedType);
  }

  /**
   * Configure the BuilderSupplier with resources and config that will be used for all the {@link
   * CommandContext} that will be created. Then called {@link
   * BuilderSupplier#getBuilder(SchemaObject)} to get a builder to configure the {@link
   * CommandContext} for the specific request.
   */
  public static class BuilderSupplier {

    private JsonProcessingMetricsReporter jsonProcessingMetricsReporter;
    private CQLSessionCache cqlSessionCache;
    private CommandConfig commandConfig;
    private EmbeddingProviderFactory embeddingProviderFactory;
    private RerankingProviderFactory rerankingProviderFactory;

    BuilderSupplier() {}

    public BuilderSupplier withJsonProcessingMetricsReporter(
        JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
      this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
      return this;
    }

    public BuilderSupplier withCqlSessionCache(CQLSessionCache cqlSessionCache) {
      this.cqlSessionCache = cqlSessionCache;
      return this;
    }

    public BuilderSupplier withCommandConfig(CommandConfig commandConfig) {
      this.commandConfig = commandConfig;
      return this;
    }

    public BuilderSupplier withEmbeddingProviderFactory(
        EmbeddingProviderFactory embeddingProviderFactory) {
      this.embeddingProviderFactory = embeddingProviderFactory;
      return this;
    }

    public BuilderSupplier withRerankingProviderFactory(
        RerankingProviderFactory rerankingProviderFactory) {
      this.rerankingProviderFactory = rerankingProviderFactory;
      return this;
    }

    public <SchemaT extends SchemaObject> Builder<SchemaT> getBuilder(SchemaT schemaObject) {

      Objects.requireNonNull(
          jsonProcessingMetricsReporter, "jsonProcessingMetricsReporter must not be null");
      Objects.requireNonNull(cqlSessionCache, "cqlSessionCache must not be null");
      Objects.requireNonNull(commandConfig, "commandConfig must not be null");
      Objects.requireNonNull(embeddingProviderFactory, "embeddingProviderFactory must not be null");
      Objects.requireNonNull(rerankingProviderFactory, "rerankingProviderFactory must not be null");

      // SchemaObject is passed here so the generics gets locked here, makes call chaining easier
      Objects.requireNonNull(schemaObject, "schemaObject must not be null");
      return new Builder<>(schemaObject);
    }

    /**
     * A builder for a {@link CommandContext} that is configured with for a specific request.
     *
     * <p>Deliberately not a static inner class, so that the {@link BuilderSupplier} does not need
     * to pass all the resources and config to the builder.
     *
     * @param <SchemaT> The schema object type that this context is for.
     */
    public class Builder<SchemaT extends SchemaObject> {

      private final SchemaT schemaObject;
      private EmbeddingProvider embeddingProvider;
      private String commandName;
      private RequestContext requestContext;
      private ApiFeatures apiFeatures;
      private MeterRegistry meterRegistry;
      private JsonApiMetricsConfig jsonApiMetricsConfig;

      Builder(SchemaT schemaObject) {
        this.schemaObject = schemaObject;
      }

      public Builder<SchemaT> withEmbeddingProvider(EmbeddingProvider embeddingProvider) {
        this.embeddingProvider = embeddingProvider;
        return this;
      }

      public Builder<SchemaT> withCommandName(String commandName) {
        this.commandName = commandName;
        return this;
      }

      public Builder<SchemaT> withRequestContext(RequestContext requestContext) {
        this.requestContext = requestContext;
        return this;
      }

      /**
       * Optional {@link ApiFeatures} that can be set when running testing, normally set to {@link
       * ApiFeatures#empty()}
       */
      public Builder<SchemaT> withApiFeatures(ApiFeatures apiFeatures) {
        this.apiFeatures = apiFeatures;
        return this;
      }

      public Builder<SchemaT> withMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        return this;
      }

      public CommandContext<SchemaT> build() {
        // embeddingProvider may be null, e.g. a keyspace command this will change when we pass in
        // all the providers
        Objects.requireNonNull(commandName, "commandName must not be null");
        Objects.requireNonNull(requestContext, "requestContext must not be null");

        return new CommandContext<>(
            schemaObject,
            embeddingProvider,
            commandName,
            requestContext,
            jsonProcessingMetricsReporter,
            cqlSessionCache,
            commandConfig,
            apiFeatures,
            embeddingProviderFactory,
            rerankingProviderFactory,
            meterRegistry);
      }
    }
  }
}
