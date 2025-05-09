package io.stargate.sgv2.jsonapi.service.operation.reranking;

import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;

/**
 * An interface for objects that can provide information about the {@link Feature}s they utilize.
 * This is typically implemented by commands to declare which features they depend on. See {@link
 * FindAndRerankCommand} as an examples
 */
public interface FeatureSource {
  /** Gets the set of features used by this source. */
  FeatureUsage getFeatureUsage();
}
