package io.stargate.sgv2.jsonapi.api.model.command;

import com.google.common.base.Preconditions;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.ApiConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Defines the context in which to execute the command.
 *
 * @param schemaObject Settings for the collection, if Collection-specific command; if not, "empty"
 *     Settings {see CollectionSettings#empty()}.
 */
public record CommandContext<T extends SchemaObject>(
    T schemaObject,
    EmbeddingProvider embeddingProvider,
    String commandName,
    JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
    RequestContext requestContext,
    CQLSessionCache cqlSessionCache) {

  private static final ConcurrentMap<Class<?>, Object> configCache = new ConcurrentHashMap<>();

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
  @SuppressWarnings("unchecked")
  public static <T extends SchemaObject> CommandContext<T> forSchemaObject(
      T schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      RequestContext requestContext,
      CQLSessionCache cqlSessionCache) {

    Objects.requireNonNull(schemaObject);

    return new CommandContext<>(
        schemaObject,
        embeddingProvider,
        commandName,
        jsonProcessingMetricsReporter,
        requestContext,
        cqlSessionCache);
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
   * Uses the config service to populate the config interface passed in.
   *
   * <p>Example: <code>
   *   bool isDebugMode = getConfig(DebugModeConfig.class).enabled()
   * </code>
   *
   * @param configType The configuration interface to populate, normally be decorated with {@link
   *     io.smallrye.config.ConfigMapping}
   * @return Populated configration object of type <code>configType</code>
   * @param <ConfigType> The configuration interface to populate
   */
  public <ConfigType> ConfigType getConfig(Class<ConfigType> configType) {
    return (ConfigType)
        configCache.computeIfAbsent(
            configType,
            k -> {
              return getConfig().getConfigMapping(configType);
            });
  }

  /**
   * Gets the config service to use, depends on the offline mode.
   *
   * <p>Exposed as we may need to get this from multiple places, best to use {@link
   * #getConfig(Class)}
   *
   * <p>TODO: Copied from JsonAPIException , not sure why we need to do this
   */
  public SmallRyeConfig getConfig() {
    // aaron - copied from JsonAPIException , not sure why we need to do this
    // TODO - cleanup how we get config, this seem unnecessary complicated

    if (ApiConstants.isOffline()) {
      // Prev code  is below, but confusing that it was then used to map different interfaces
      // config = new SmallRyeConfigBuilder().withMapping(DebugModeConfig.class).build();
      return new SmallRyeConfigBuilder().build();
    }
    return ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
  }

  public OperationsConfig operationsConfig() {
    return getConfig(OperationsConfig.class);
  }
}
