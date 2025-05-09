package io.stargate.sgv2.jsonapi.service.operation.reranking;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.stream.Collectors;

public final class FeatureUsage {
  private final EnumSet<Feature> features;

  public static final FeatureUsage EMPTY = new FeatureUsage(EnumSet.noneOf(Feature.class));

  private FeatureUsage(EnumSet<Feature> features) {
    this.features = EnumSet.copyOf(features);
  }

  public static FeatureUsage of(Feature... features) {
    if (features == null || features.length == 0) {
      return EMPTY;
    }
    EnumSet<Feature> set = EnumSet.noneOf(Feature.class);
    Collections.addAll(set, features);
    return new FeatureUsage(set);
  }

  public static FeatureUsage of(EnumSet<Feature> features) {
    if (features == null || features.isEmpty()) {
      return EMPTY;
    }
    return new FeatureUsage(features);
  }

  public void add(Feature feature) {
    Objects.requireNonNull(feature);
    this.features.add(feature);
  }

  public void add(FeatureUsage other) {
    if (other == null || other.features.isEmpty() || other == this) {
      return;
    }
    this.features.addAll(other.features);
  }

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
