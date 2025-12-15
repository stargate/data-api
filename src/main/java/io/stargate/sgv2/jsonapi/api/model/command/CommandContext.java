package io.stargate.sgv2.jsonapi.api.model.command;

import com.google.common.base.Preconditions;
import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.DefaultRequestTracing;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.stargate.sgv2.jsonapi.logging.LoggingMDCContext;
import io.stargate.sgv2.jsonapi.metrics.CommandFeatures;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectType;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import java.util.ArrayList;
import java.util.List;
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
 * <p><b>NOTE:</b> When {@link BuilderSupplier.Builder#build()} is called it will call {@link
 * #addToMDC()} so that the context is added to the logging MDC for the duration of the request. The
 * context must be closed via {@link #close()} to remove it from the MDC, this should be done at the
 * last possible time in the resource handler so all log messages have the context.
 *
 * @param <SchemaT> The schema object type that this context is for. There are times we need to lock
 *     this down to the specific type, if so use the "as" methods such as {@link
 *     CommandContext#asCollectionContext()}
 */
public class CommandContext<SchemaT extends SchemaObject> implements LoggingMDCContext {

  // Common for all instances
  private final JsonProcessingMetricsReporter jsonProcessingMetricsReporter;
  private final CQLSessionCache cqlSessionCache;
  private final CommandConfig commandConfig;
  private final EmbeddingProviderFactory embeddingProviderFactory;
  private final RerankingProviderFactory rerankingProviderFactory;
  private final MeterRegistry meterRegistry;

  // Request specific
  private final SchemaT schemaObject;
  private final RequestTracing requestTracing;
  private final RequestContext requestContext;
  private final EmbeddingProvider
      embeddingProvider; // to be removed later, this is a single provider
  private final String commandName; // TODO: remove the command name, but it is used in 14 places

  // both per request list of objects that want to update the logging MDC context,
  // add to this list in the ctor. See {@link #addToMDC()} and {@link #removeFromMDC()}
  private final List<LoggingMDCContext> loggingMDCContexts = new ArrayList<>();

  // see accessors
  private FindAndRerankCommand.HybridLimits hybridLimits;

  // used to track the features used in the command
  private final CommandFeatures commandFeatures;

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

    // Common for all instances
    this.cqlSessionCache = cqlSessionCache;
    this.commandConfig = commandConfig;
    this.embeddingProviderFactory = embeddingProviderFactory;
    this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
    this.meterRegistry = meterRegistry;
    this.rerankingProviderFactory = rerankingProviderFactory;

    // Request specific
    this.embeddingProvider = embeddingProvider; // to be removed later, this is a single provider
    this.requestContext = requestContext;
    this.schemaObject = schemaObject;
    this.commandName = commandName; // TODO: remove the command name, but it is used in 14 places
    this.apiFeatures = apiFeatures;

    this.loggingMDCContexts.add(this.requestContext);
    this.loggingMDCContexts.add(this.schemaObject.identifier());

    var anyTracing =
        apiFeatures().isFeatureEnabled(ApiFeature.REQUEST_TRACING)
            || apiFeatures().isFeatureEnabled(ApiFeature.REQUEST_TRACING_FULL);

    this.requestTracing =
        anyTracing
            ? new DefaultRequestTracing(
                requestContext.requestId(),
                requestContext.tenant(),
                apiFeatures().isFeatureEnabled(ApiFeature.REQUEST_TRACING_FULL))
            : RequestTracing.NO_OP;

    this.commandFeatures = CommandFeatures.create();
  }

  /** See doc comments for {@link CommandContext} */
  public static BuilderSupplier builderSupplier() {
    return new BuilderSupplier();
  }

  /**
   * HACK: for https://github.com/stargate/data-api/issues/1961 This is a temporary work around for
   * needing to pass the page size to the FindCollectionOperation when doing the inner finds for
   * findAndRerank because they will only run the command once, and not multiple times to exhaust
   * the cursor.
   *
   * @return
   */
  public FindAndRerankCommand.HybridLimits getHybridLimits() {
    return hybridLimits;
  }

  public void setHybridLimits(FindAndRerankCommand.HybridLimits hybridLimits) {
    this.hybridLimits = hybridLimits;
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

  public CommandFeatures commandFeatures() {
    return commandFeatures;
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
    return schemaObject().type() == SchemaObjectType.COLLECTION;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<CollectionSchemaObject> asCollectionContext() {
    checkSchemaObjectType(SchemaObjectType.COLLECTION);
    return (CommandContext<CollectionSchemaObject>) this;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<TableSchemaObject> asTableContext() {
    checkSchemaObjectType(SchemaObjectType.TABLE);
    return (CommandContext<TableSchemaObject>) this;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<KeyspaceSchemaObject> asKeyspaceContext() {
    checkSchemaObjectType(SchemaObjectType.KEYSPACE);
    return (CommandContext<KeyspaceSchemaObject>) this;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<DatabaseSchemaObject> asDatabaseContext() {
    checkSchemaObjectType(SchemaObjectType.DATABASE);
    return (CommandContext<DatabaseSchemaObject>) this;
  }

  private void checkSchemaObjectType(SchemaObjectType expectedType) {
    Preconditions.checkArgument(
        schemaObject().type() == expectedType,
        "SchemaObject type actual was %s expected was %s ",
        schemaObject().type(),
        expectedType);
  }

  @Override
  public void addToMDC() {
    loggingMDCContexts.forEach(LoggingMDCContext::addToMDC);
  }

  @Override
  public void removeFromMDC() {
    loggingMDCContexts.forEach(LoggingMDCContext::removeFromMDC);
  }

  /**
   * NOTE: Not using AutoCloseable because it created a lot of linting warnings, we only want to
   * close this in the request resource handler.
   */
  public void close() throws Exception {
    removeFromMDC();
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
    private MeterRegistry meterRegistry;

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

    public BuilderSupplier withMeterRegistry(MeterRegistry meterRegistry) {
      this.meterRegistry = meterRegistry;
      return this;
    }

    public <SchemaT extends SchemaObject> Builder<SchemaT> getBuilder(SchemaT schemaObject) {

      Objects.requireNonNull(
          jsonProcessingMetricsReporter, "jsonProcessingMetricsReporter must not be null");
      Objects.requireNonNull(cqlSessionCache, "cqlSessionCache must not be null");
      Objects.requireNonNull(commandConfig, "commandConfig must not be null");
      Objects.requireNonNull(embeddingProviderFactory, "embeddingProviderFactory must not be null");
      Objects.requireNonNull(rerankingProviderFactory, "rerankingProviderFactory must not be null");
      Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");

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

      public CommandContext<SchemaT> build() {
        // embeddingProvider may be null, e.g. a keyspace command this will change when we pass in
        // all the providers
        Objects.requireNonNull(commandName, "commandName must not be null");
        Objects.requireNonNull(requestContext, "requestContext must not be null");

        var context =
            new CommandContext<>(
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
        context.addToMDC();
        return context;
      }
    }
  }
}
