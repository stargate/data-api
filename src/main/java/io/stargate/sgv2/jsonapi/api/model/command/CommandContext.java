package io.stargate.sgv2.jsonapi.api.model.command;

import com.google.common.base.Preconditions;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.constants.ApiConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
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
    ApiFeatures apiFeatures) {

  // TODO: this is what the original EMPTY had, no idea why the name of the command is missing
  // this is used by the GeneralResource
  //  private static final CommandContext EMPTY =
  //      new CommandContext(null, null, CollectionSettings.empty(), null, "testCommand", null);

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
      ApiFeatures apiFeatures) {

    // TODO: upgrade to use the modern switch statements
    // TODO: how to remove the unchecked cast ? Had to use unchecked cast to get back to the
    // CommandContext<T>
    if (schemaObject instanceof CollectionSchemaObject cso) {
      return (CommandContext<T>)
          forSchemaObject(
              cso, embeddingProvider, commandName, jsonProcessingMetricsReporter, apiFeatures);
    }
    if (schemaObject instanceof TableSchemaObject tso) {
      return (CommandContext<T>)
          forSchemaObject(
              tso, embeddingProvider, commandName, jsonProcessingMetricsReporter, apiFeatures);
    }
    if (schemaObject instanceof KeyspaceSchemaObject kso) {
      return (CommandContext<T>)
          forSchemaObject(
              kso, embeddingProvider, commandName, jsonProcessingMetricsReporter, apiFeatures);
    }
    if (schemaObject instanceof DatabaseSchemaObject dso) {
      return (CommandContext<T>)
          forSchemaObject(
              dso, embeddingProvider, commandName, jsonProcessingMetricsReporter, apiFeatures);
    }
    throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
        "Unknown schema object type: %s", schemaObject.getClass().getName());
  }

  /**
   * Factory method to create a new instance of {@link CommandContext} based on the schema object we
   * are working with
   *
   * @param schemaObject
   * @param embeddingProvider
   * @param commandName
   * @param jsonProcessingMetricsReporter
   * @return
   */
  public static CommandContext<CollectionSchemaObject> forSchemaObject(
      CollectionSchemaObject schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      ApiFeatures apiFeatures) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter, apiFeatures);
  }

  /**
   * Factory method to create a new instance of {@link CommandContext} based on the schema object we
   * are working with
   *
   * @param schemaObject
   * @param embeddingProvider
   * @param commandName
   * @param jsonProcessingMetricsReporter
   * @return
   */
  public static CommandContext<TableSchemaObject> forSchemaObject(
      TableSchemaObject schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      ApiFeatures apiFeatures) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter, apiFeatures);
  }

  /**
   * Factory method to create a new instance of {@link CommandContext} based on the schema object we
   * are working with
   *
   * @param schemaObject
   * @param embeddingProvider
   * @param commandName
   * @param jsonProcessingMetricsReporter
   * @return
   */
  public static CommandContext<KeyspaceSchemaObject> forSchemaObject(
      KeyspaceSchemaObject schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      ApiFeatures apiFeatures) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter, apiFeatures);
  }

  /**
   * Factory method to create a new instance of {@link CommandContext} based on the schema object we
   * are working with
   *
   * @param schemaObject
   * @param embeddingProvider
   * @param commandName
   * @param jsonProcessingMetricsReporter
   * @return
   */
  public static CommandContext<DatabaseSchemaObject> forSchemaObject(
      DatabaseSchemaObject schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      ApiFeatures apiFeatures) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter, apiFeatures);
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
    return getConfig().getConfigMapping(configType);
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
}
