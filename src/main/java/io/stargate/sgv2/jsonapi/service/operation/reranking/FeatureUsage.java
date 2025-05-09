package io.stargate.sgv2.jsonapi.service.operation.reranking;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a collection of {@link Feature}s in a command. This class is immutable and can be used
 * to track which features are utilized in a command. This class is immutable; methods that modify
 * the set of features return a new {@code FeatureUsage} instance. It uses an {@link EnumSet}
 * internally for efficient storage and operations on features.
 */
public final class FeatureUsage {
  private final EnumSet<Feature> features;

  /** A instance representing no features in use. */
  public static final FeatureUsage EMPTY = new FeatureUsage(EnumSet.noneOf(Feature.class));

  private FeatureUsage(EnumSet<Feature> features) {
    this.features = EnumSet.copyOf(features);
  }

  /**
   * Creates a {@code FeatureUsage} instance from an array of {@link Feature}s.
   *
   * @param features The features to include. If null or empty, {@link #EMPTY} is returned.
   * @return A new {@code FeatureUsage} instance containing the specified features.
   */
  public static FeatureUsage of(Feature... features) {
    if (features == null || features.length == 0) {
      return EMPTY;
    }
    EnumSet<Feature> set = EnumSet.noneOf(Feature.class);
    Collections.addAll(set, features);
    return new FeatureUsage(set);
  }

  /**
   * Creates a {@code FeatureUsage} instance from an {@link EnumSet} of {@link Feature}s.
   *
   * @param features The set of features to include. If null or empty, {@link #EMPTY} is returned.
   * @return A new {@code FeatureUsage} instance containing the specified features.
   */
  public static FeatureUsage of(EnumSet<Feature> features) {
    if (features == null || features.isEmpty()) {
      return EMPTY;
    }
    return new FeatureUsage(features);
  }

  /**
   * Returns a new {@code FeatureUsage} instance that includes the specified feature in addition to
   * the features in this instance. If the feature is already present, this instance is returned.
   *
   * @param feature The feature to add. Must not be null.
   * @return A new {@code FeatureUsage} instance with the added feature, or this instance if the
   *     feature was already present.
   * @throws NullPointerException if the feature is null.
   */
  public FeatureUsage withFeature(Feature feature) {
    Objects.requireNonNull(feature, "Feature cannot be null");
    if (this.features.contains(feature)) {
      return this;
    }
    EnumSet<Feature> newSet = EnumSet.copyOf(this.features);
    newSet.add(feature);
    return new FeatureUsage(newSet);
  }

  /**
   * Returns a new {@code FeatureUsage} instance that is the union of this instance's features and
   * the features of another {@code FeatureUsage} instance.
   *
   * @param other The other {@code FeatureUsage} instance to combine with. If null, empty, or the
   *     same as this instance, this instance is returned. If this instance is empty, the other
   *     instance is returned.
   * @return A new {@code FeatureUsage} instance representing the combined set of features.
   */
  public FeatureUsage unionWith(FeatureUsage other) {
    if (other == null || other.features.isEmpty() || other == this) {
      return this;
    }
    if (this.features.isEmpty()) {
      return other;
    }
    EnumSet<Feature> combined = EnumSet.copyOf(this.features);
    combined.addAll(other.features);
    return new FeatureUsage(combined);
  }

  /**
   * Gets a copy of the set of features contained in this {@code FeatureUsage}. The returned set is
   * a defensive copy and modifications to it will not affect this {@code FeatureUsage} instance.
   *
   * @return An {@link EnumSet} containing the features.
   */
  public EnumSet<Feature> getFeatures() {
    return EnumSet.copyOf(features);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    FeatureUsage that = (FeatureUsage) obj;
    return Objects.equals(features, that.features);
  }

  @Override
  public int hashCode() {
    return Objects.hash(features);
  }

  @Override
  public String toString() {
    return "FeatureUsage{"
        + features.stream().map(Enum::name).collect(Collectors.joining(", "))
        + '}';
  }
}
