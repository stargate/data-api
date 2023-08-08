package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.service.bridge.executor.NamespaceCache;

/**
 * Defines the context in which to execute the command.
 *
 * @param namespace The name of the namespace.
 * @param collection The name of the collection.
 */
public record CommandContext(
    String namespace,
    String collection,
    boolean isVectorEnabled,
    NamespaceCache.CollectionProperty.SimilarityFunction similarityFunction) {

  public CommandContext(String namespace, String collection) {
    this(namespace, collection, false, null);
  }

  private static final CommandContext EMPTY = new CommandContext(null, null, false, null);

  /**
   * @return Returns empty command context, having both {@link #namespace} and {@link #collection}
   *     as <code>null</code>.
   */
  public static CommandContext empty() {
    return EMPTY;
  }
}
