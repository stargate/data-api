package io.stargate.sgv2.jsonapi.api.model.command;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;

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
    JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {

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
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {

    // TODO: upgrade to use the modern switch statements
    // TODO: how to remove the unchecked cast ? Had to use unchecked cast to get back to the
    // CommandContext<T>
    if (schemaObject instanceof CollectionSchemaObject cso) {
      return (CommandContext<T>)
          forSchemaObject(cso, embeddingProvider, commandName, jsonProcessingMetricsReporter);
    }
    if (schemaObject instanceof TableSchemaObject tso) {
      return (CommandContext<T>)
          forSchemaObject(tso, embeddingProvider, commandName, jsonProcessingMetricsReporter);
    }
    if (schemaObject instanceof KeyspaceSchemaObject kso) {
      return (CommandContext<T>)
          forSchemaObject(kso, embeddingProvider, commandName, jsonProcessingMetricsReporter);
    }
    if (schemaObject instanceof DatabaseSchemaObject dso) {
      return (CommandContext<T>)
          forSchemaObject(dso, embeddingProvider, commandName, jsonProcessingMetricsReporter);
    }
    throw new IllegalArgumentException("Unknown schema object type: " + schemaObject.getClass());
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
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter);
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
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter);
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
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter);
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
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter);
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
        schemaObject().type == expectedType,
        "SchemaObject type actual was %s expected was %s ",
        schemaObject().type,
        expectedType);
  }

  // TODO: why do we have these public ctors, and a static factor, and this is a record ??
  //  public CommandContext(String namespace, String collection) {
  //    this(CollectionSchemaObject.EMPTY, null, null, null);
  //  }

  //  public CommandContext(
  //      String namespace,
  //      String collection,
  //      String commandName,
  //      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
  //    this(
  //        CollectionSchemaObject.EMPTY,
  //        null,
  //        commandName,
  //        jsonProcessingMetricsReporter);
  //  }

  /**
   * An utility method to create a new instance of {@link CommandContext} by passing the namespace,
   * the collection, the collection settings, embedding provider and the command name.
   *
   * @param namespace Namespace
   * @param collection Collection
   * @param collectionSettings Collection settings
   * @param embeddingProvider Embedding provider
   * @param commandName Command name
   * @return Returns a new instance of {@link CommandContext}.
   */
  //  public static CommandContext from(
  //      String namespace,
  //      String collection,
  //      CollectionSchemaObject collectionSettings,
  //      EmbeddingProvider embeddingProvider,
  //      String commandName) {
  //    return new CommandContext(
  //        collectionSettings, embeddingProvider, commandName, null);
  //  }

}
