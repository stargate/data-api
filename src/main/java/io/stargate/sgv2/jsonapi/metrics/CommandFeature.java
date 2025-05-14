package io.stargate.sgv2.jsonapi.metrics;

import java.util.Objects;

/**
 * Represents distinct features that can be used in the general command. Each feature has an
 * associated tag name, which can be used for metrics.
 */
public enum CommandFeature {
  /** The usage of $lexical in the command */
  LEXICAL("feature.lexical"),
  /** The usage of $vector in the command */
  VECTOR("feature.vector"),
  /** The usage of $vectorize in the command */
  VECTORIZE("feature.vectorize"),

  /** The usage of $hybrid with String in the command */
  HYBRID("feature.hybrid.string"),

  /** The usage of `hybridLimits` with Number in the command */
  HYBRID_LIMITS_NUMBER("feature.hybrid.limits.number"),
  /** The usage of `hybridLimits` Object with $vector in the command */
  HYBRID_LIMITS_VECTOR("feature.hybrid.limits.vector"),
  /** The usage of `hybridLimits` Object with $lexical in the command */
  HYBRID_LIMITS_LEXICAL("feature.hybrid.limits.lexical"),
  ;

  private final String tagName;

  CommandFeature(String tagName) {
    this.tagName = Objects.requireNonNull(tagName);
  }

  public String getTagName() {
    return tagName;
  }
}
