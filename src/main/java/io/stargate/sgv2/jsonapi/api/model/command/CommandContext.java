package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;

/**
 * Defines the context in which to execute the command.
 *
 * @param schemaObject Settings for the collection, if Collection-specific command; if not,
 *                           "empty" Settings {see CollectionSettings#empty()}.
 */
public record CommandContext(
    SchemaObject schemaObject,
    EmbeddingProvider embeddingProvider,
    String commandName,
    JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {

  private static final CommandContext EMPTY =
      new CommandContext(CollectionSchemaObject.EMPTY, null, "testCommand", null);

  /**
   * @return Returns empty command context, having both {@link #namespace} and {@link #collection}
   *     as <code>null</code>.
   */
  public static CommandContext empty() {
    return EMPTY;
  }

  // TODO: why do we have these public ctors, and a static factor, and this is a record ??
  public CommandContext(String namespace, String collection) {
    this(CollectionSchemaObject.EMPTY, null, null, null);
  }

  public CommandContext(
      String namespace,
      String collection,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    this(
        CollectionSchemaObject.EMPTY,
        null,
        commandName,
        jsonProcessingMetricsReporter);
  }

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
  public static CommandContext from(
      String namespace,
      String collection,
      CollectionSchemaObject collectionSettings,
      EmbeddingProvider embeddingProvider,
      String commandName) {
    return new CommandContext(
        collectionSettings, embeddingProvider, commandName, null);
  }

  // TODO: these helper functions break encapsulation for very little benefit
  public CollectionSchemaObject.SimilarityFunction similarityFunction() {
    // HACK AARON - temp cast until I work out if we generic the command context
    return ((CollectionSchemaObject)schemaObject).vectorConfig().similarityFunction();
  }

  public boolean isVectorEnabled() {
    // HACK AARON - temp cast until I work out if we generic the command context
    return ((CollectionSchemaObject)schemaObject).vectorConfig() != null
        && ((CollectionSchemaObject)schemaObject).vectorConfig().vectorEnabled();
  }

  public IndexingProjector indexingProjector() {
    // HACK AARON - temp cast until I work out if we generic the command context
    return ((CollectionSchemaObject)schemaObject).indexingProjector();
  }
}
