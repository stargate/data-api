package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;

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
    EmbeddingService embeddingService,
    String commandName,
    JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {

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

  private static final CommandContext EMPTY =
      new CommandContext(null, null, CollectionSettings.empty(), null, "testCommand", null);

  /**
   * @return Returns empty command context, having both {@link #namespace} and {@link #collection}
   *     as <code>null</code>.
   */
  public static CommandContext empty() {
    return EMPTY;
  }

  public CollectionSettings.SimilarityFunction similarityFunction() {
    return collectionSettings.similarityFunction();
  }

  public boolean isVectorEnabled() {
    return collectionSettings.vectorEnabled();
  }

  public DocumentProjector indexingProjector() {
    return collectionSettings.indexingProjector();
  }
}
