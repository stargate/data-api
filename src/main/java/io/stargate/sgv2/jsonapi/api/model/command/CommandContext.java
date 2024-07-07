package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.projection.IndexingProjector;

/**
 * Defines the context in which to execute the command.
 *
 * @param namespace The name of the namespace.
 * @param collection The name of the collection.
 * @param collectionSettings Settings for the collection, if Collection-specific command; if not,
 *     "empty" Settings {see CollectionSettings#empty()}.
 */
public record CommandContext(
    String namespace,
    String collection,
    CollectionSettings collectionSettings,
    EmbeddingProvider embeddingProvider,
    String commandName,
    JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {

  private static final CommandContext EMPTY =
      new CommandContext(null, null, CollectionSettings.empty(), null, "testCommand", null);

  /**
   * @return Returns empty command context, having both {@link #namespace} and {@link #collection}
   *     as <code>null</code>.
   */
  public static CommandContext empty() {
    return EMPTY;
  }

  // TODO: why do we have these public ctors, and a static factor, and this is a record ??
  public CommandContext(String namespace, String collection) {
    this(namespace, collection, CollectionSettings.empty(), null, null, null);
  }

  public CommandContext(
      String namespace,
      String collection,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    this(
        namespace,
        collection,
        CollectionSettings.empty(),
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
      CollectionSettings collectionSettings,
      EmbeddingProvider embeddingProvider,
      String commandName) {
    return new CommandContext(
        namespace, collection, collectionSettings, embeddingProvider, commandName, null);
  }

  // TODO: these helpder functions break encapsulation for very little benefit
  public CollectionSettings.SimilarityFunction similarityFunction() {
    return collectionSettings.vectorConfig().similarityFunction();
  }

  public boolean isVectorEnabled() {
    return collectionSettings.vectorConfig() != null
        && collectionSettings.vectorConfig().vectorEnabled();
  }

  public IndexingProjector indexingProjector() {
    return collectionSettings.indexingProjector();
  }
}
