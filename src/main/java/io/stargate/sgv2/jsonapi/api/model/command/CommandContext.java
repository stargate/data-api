package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import java.util.List;

/**
 * Defines the context in which to execute the command.
 *
 * @param namespace The name of the namespace.
 * @param collection The name of the collection.
 * @param isVectorEnabled Whether the vector is enabled for the collection
 * @param similarityFunction The similarity function used for indexing the vector
 */
public record CommandContext(
    String namespace,
    String collection,
    boolean isVectorEnabled,
    CollectionSettings.SimilarityFunction similarityFunction,
    EmbeddingService embeddingService,
    CollectionSettings.IndexingConfig indexingConfig) {

  public CommandContext(String namespace, String collection) {
    this(namespace, collection, false, null, null, null);
  }

  private static final CommandContext EMPTY =
      new CommandContext(null, null, false, null, null, null);

  /**
   * @return Returns empty command context, having both {@link #namespace} and {@link #collection}
   *     as <code>null</code>.
   */
  public static CommandContext empty() {
    return EMPTY;
  }

  public void tryVectorize(JsonNodeFactory nodeFactory, List<JsonNode> documents) {
    new DataVectorizer(embeddingService(), nodeFactory).vectorize(documents);
  }

  public void tryVectorize(JsonNodeFactory nodeFactory, SortClause sortClause) {
    new DataVectorizer(embeddingService(), nodeFactory).vectorize(sortClause);
  }

  public void tryVectorize(JsonNodeFactory nodeFactory, UpdateClause updataClause) {
    new DataVectorizer(embeddingService(), nodeFactory).vectorizeUpdateClause(updataClause);
  }
}
