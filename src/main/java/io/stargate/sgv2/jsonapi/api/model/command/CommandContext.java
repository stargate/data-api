package io.stargate.sgv2.jsonapi.api.model.command;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
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

  // Request specific
  private final SchemaT schemaObject;
  private final EmbeddingProvider
      embeddingProvider; // to be removed later, this is a single provider
  private final String commandName; // TODO: remove the command name, but it is used in 14 places
  private final RequestContext requestContext;
  private final ApiFeatures apiFeatures;

  private CommandContext(
      SchemaT schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      RequestContext requestContext,
      ApiFeatures apiFeatures,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      CQLSessionCache cqlSessionCache,
      CommandConfig commandConfig) {

    this.schemaObject = schemaObject;
    this.embeddingProvider = embeddingProvider;
    this.commandName = commandName;
    this.requestContext = requestContext;
    this.apiFeatures = apiFeatures;

    this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
    this.cqlSessionCache = cqlSessionCache;
    this.commandConfig = commandConfig;
  }

  public static BuilderSupplier builderSupplier() {
    return new BuilderSupplier();
  }

  public SchemaT schemaObject() {
    return schemaObject;
  }

  public EmbeddingProvider embeddingProvider() {
    return embeddingProvider;
  }

  public String commandName() {
    return commandName;
  }

  public RequestContext requestContext() {
    return requestContext;
  }

  public ApiFeatures apiFeatures() {
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

    public <SchemaT extends SchemaObject> Builder<SchemaT> getBuilder(SchemaT schemaObject) {

      Objects.requireNonNull(
          jsonProcessingMetricsReporter, "jsonProcessingMetricsReporter must not be null");
      Objects.requireNonNull(cqlSessionCache, "cqlSessionCache must not be null");
      Objects.requireNonNull(commandConfig, "commandConfig must not be null");

      // SchemaObject is passed here so the generics gets locked here, makes call chaining easier
      Objects.requireNonNull(schemaObject, "schemaObject must not be null");
      return new Builder<>(schemaObject);
    }

    /**
     * A builder for a {@link CommandContext} that is configured with for a specific request.
     *
     * @param <SchemaT> The schema object type that this context is for.
     */
    public class Builder<SchemaT extends SchemaObject> {

      private final SchemaT schemaObject;
      private EmbeddingProvider embeddingProvider;
      private String commandName;
      private RequestContext requestContext;

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

      public CommandContext<SchemaT> build() {
        // embeddingProvider may be null, e.g. a keyspace command this will change when we pass in
        // all the providers
        Objects.requireNonNull(commandName, "commandName must not be null");
        Objects.requireNonNull(requestContext, "requestContext must not be null");

        // Merging the config for features with the request headers to get the final feature set
        var apiFeatures =
            ApiFeatures.fromConfigAndRequest(
                commandConfig.get(FeaturesConfig.class), requestContext.getHttpHeaders());

        return new CommandContext<>(
            schemaObject,
            embeddingProvider,
            commandName,
            requestContext,
            apiFeatures,
            jsonProcessingMetricsReporter,
            cqlSessionCache,
            commandConfig);
      }
    }
  }
}
