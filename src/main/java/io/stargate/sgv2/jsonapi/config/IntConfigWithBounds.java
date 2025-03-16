package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.WithConverter;

/**
 * An integer value that has a min, default, and max.
 * <p>
 * For example, the hybrid find limits for vector and lexical reads.
 */
public record IntConfigWithBounds(int min, int defaultValue, int max) {

  public IntConfigWithBounds{
    if (min > defaultValue || defaultValue > max) {
      throw new IllegalArgumentException("min, defaultValue, and max must be in order, got min=" + min + ", defaultValue=" + defaultValue + ", max=" + max);
    }
  }
  public boolean isValid(int value) {
    return value >= min && value <= max;
  }

}
