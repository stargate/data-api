package io.stargate.sgv2.jsonapi.api.model.command;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
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

  public static final CommandContext<CollectionSchemaObject> EMPTY_COLLECTION =
      new CommandContext<>(CollectionSchemaObject.MISSING, null, "testCommand", null);

  public static final CommandContext<TableSchemaObject> EMPTY_TABLE =
      new CommandContext<>(TableSchemaObject.MISSING, null, "testCommand", null);

  public static CommandContext<CollectionSchemaObject> collectionCommandContext(
      CollectionSchemaObject schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter);
  }

  public static CommandContext<TableSchemaObject> tableCommandContext(
      TableSchemaObject schemaObject,
      EmbeddingProvider embeddingProvider,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    return new CommandContext<>(
        schemaObject, embeddingProvider, commandName, jsonProcessingMetricsReporter);
  }

  @SuppressWarnings("unchecked")
  public CommandContext<CollectionSchemaObject> asCollectionContext() {
    Preconditions.checkArgument(
        schemaObject().type == CollectionSchemaObject.TYPE,
        "SchemaObject type is actual was %s expected was %s ",
        schemaObject().type,
        CollectionSchemaObject.TYPE);
    return (CommandContext<CollectionSchemaObject>) this;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<TableSchemaObject> asTableContext() {
    Preconditions.checkArgument(
        schemaObject().type == TableSchemaObject.TYPE,
        "SchemaObject type is actual was %s expected was %s ",
        schemaObject().type,
        TableSchemaObject.TYPE);
    return (CommandContext<TableSchemaObject>) this;
  }

  @SuppressWarnings("unchecked")
  public CommandContext<KeyspaceSchemaObject> asKeyspaceContext() {
    Preconditions.checkArgument(
        schemaObject().type == KeyspaceSchemaObject.TYPE,
        "SchemaObject type is actual was %s expected was %s ",
        schemaObject().type,
        KeyspaceSchemaObject.TYPE);
    return (CommandContext<KeyspaceSchemaObject>) this;
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
