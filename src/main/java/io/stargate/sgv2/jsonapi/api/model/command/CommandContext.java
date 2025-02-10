package io.stargate.sgv2.jsonapi.api.model.command;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.Objects;

/**
 * Defines the context in which to execute the command.
 *
 * @param schemaObject Settings for the collection, if Collection-specific command; if not, "empty"
 *     Settings {see CollectionSettings#empty()}.
 */
public class CommandContext<T extends SchemaObject> {

  // Common for all instances
  private final JsonProcessingMetricsReporter jsonProcessingMetricsReporter;
  private final CQLSessionCache cqlSessionCache;
  private CommandConfig commandConfig;

  // Request specific
  private final T schemaObject;
  private final EmbeddingProvider
      embeddingProvider; // to be removed later,  this is a single provider we want all
  private final String commandName; // TODO: remove the command name, but it is used in 14 places
  private final RequestContext requestContext;

  private CommandContext(
      T schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      RequestContext requestContext,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      CQLSessionCache cqlSessionCache) {

    this.schemaObject = schemaObject;
    this.embeddingProvider = embeddingProvider;
    this.commandName = commandName;
    this.requestContext = requestContext;

    this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
    this.cqlSessionCache = cqlSessionCache;
  }

  public static BuilderSupplier builderSupplier() {
    return new BuilderSupplier();
  }

  public T schemaObject() {
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

  public JsonProcessingMetricsReporter jsonProcessingMetricsReporter() {
    return jsonProcessingMetricsReporter;
  }

  public CQLSessionCache cqlSessionCache() {
    return cqlSessionCache;
  }

  public CommandConfig config() {
    return commandConfig;
  }

  /**
   * Factory method to create a new instance of {@link CommandContext} based on the schema object we
   * are working with.
   *
   * <p>This one handles the super class of {@link SchemaObject}
   *
   * @param schemaObject
   * @param embeddingProvider
   * @param commandName
   * @param jsonProcessingMetricsReporter
   * @return
   */
  //  @SuppressWarnings("unchecked")
  //  public static <T extends SchemaObject> CommandContext<T> forSchemaObject(
  //      T schemaObject,
  //      EmbeddingProvider embeddingProvider,
  //      String commandName,
  //      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
  //      RequestContext requestContext,
  //      CQLSessionCache cqlSessionCache) {
  //
  //    Objects.requireNonNull(schemaObject);
  //
  //    return new CommandContext<>(
  //        schemaObject,
  //        embeddingProvider,
  //        commandName,
  //        jsonProcessingMetricsReporter,
  //        requestContext,
  //        cqlSessionCache);
  //  }

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
   * CommandContext} that will be created. Then called {@link BuilderSupplier#getBuilder()} to get a
   * builder to configure the {@link CommandContext} for the specific request.
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

      // TODO: old code allowed the jsonProcessingMetricsReporter to be null, make it required
      // TODO: testing needs to pass a null cqlSessionCache make it required
      Objects.requireNonNull(cqlSessionCache, "cqlSessionCache must not be null");
      Objects.requireNonNull(commandConfig, "commandConfig must not be null");

      // SchemaObject is passed here so the genrics gets locked here, makes call chaining easier
      Objects.requireNonNull(schemaObject, "schemaObject must not be null");
      return new Builder<>(schemaObject);
    }

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

        return new CommandContext<>(
            schemaObject,
            embeddingProvider,
            commandName,
            requestContext,
            jsonProcessingMetricsReporter,
            cqlSessionCache);
      }
    }
  }
}
