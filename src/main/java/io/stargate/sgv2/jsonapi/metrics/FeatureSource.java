package io.stargate.sgv2.jsonapi.metrics;

import io.stargate.sgv2.jsonapi.api.model.command.impl.FindAndRerankCommand;

/**
 * An interface for objects that can provide information about the {@link CommandFeature}s they
 * utilize. This is typically implemented by commands to declare which features they depend on. See
 * {@link FindAndRerankCommand} as an example
 */
public interface FeatureSource {
  /** Gets the set of features used by this source. */
  CommandFeatures getCommandFeatures();
}
