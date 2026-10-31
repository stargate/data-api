package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.EnumSet;
import java.util.Objects;

/**
 * Represents a collection of {@link CommandFeature}s used in a command. This class is mutable and
 * designed to be used within a {@link io.stargate.sgv2.jsonapi.api.model.command.CommandContext} to
 * accumulate features during command processing. Mutation is controlled via specific add methods.
 * It uses an {@link EnumSet} internally for efficient storage and operations on commandFeatures.
 */
public final class CommandFeatures {

  /** An instance representing no commandFeatures in use. */
  public static final CommandFeatures EMPTY =
      new CommandFeatures(EnumSet.noneOf(CommandFeature.class));

  private static final EnumSet<CommandFeature> ALL_FEATURES = EnumSet.allOf(CommandFeature.class);

  private final EnumSet<CommandFeature> commandFeatures;

  /** Private constructor, use factory methods 'of' or 'create' */
  private CommandFeatures(EnumSet<CommandFeature> commandFeatures) {
    this.commandFeatures = commandFeatures;
  }

  /**
   * Creates a new, mutable {@code CommandFeatures} instance containing no features.
   *
   * @return A new, empty, mutable {@code CommandFeatures} instance.
   */
  public static CommandFeatures create() {
    return new CommandFeatures(EnumSet.noneOf(CommandFeature.class));
  }

  /**
   * Creates a {@code CommandFeatures} instance from an array of {@link CommandFeature}s. The
   * returned instance will be mutable.
   *
   * @param initialFeatures The initial features to include. If null or empty, an empty instance is
   *     returned.
   * @return A new {@code CommandFeatures} instance containing the specified features.
   */
  public static CommandFeatures of(CommandFeature... initialFeatures) {
    if (initialFeatures == null || initialFeatures.length == 0) {
      return create();
    }
    return new CommandFeatures(EnumSet.of(initialFeatures[0], initialFeatures));
  }

  /** Adds the specified feature to this instance. */
  public void addFeature(CommandFeature commandFeature) {
    Objects.requireNonNull(commandFeature, "CommandFeature cannot be null");
    commandFeatures.add(commandFeature);
  }

  /**
   * Adds all features from another {@code CommandFeatures} instance to this instance. Mutates the
   * current object.
   *
   * @param other The other {@code CommandFeatures} instance whose features should be added. If null
   *     or empty, this instance remains unchanged.
   */
  public void addAll(CommandFeatures other) {
    if (other != null && !other.isEmpty()) {
      commandFeatures.addAll(other.commandFeatures);
    }
  }

  /**
   * Checks if this instance contains any features.
   *
   * @return {@code true} if no features are present, {@code false} otherwise.
   */
  public boolean isEmpty() {
    return commandFeatures.isEmpty();
  }

  /**
   * Generates Micrometer Tags representing the features in this instance.
   *
   * <p>Every {@linkCommandFeature} is emitted as a tag, with value {@code true} if used and {@code
   * false} otherwise. Prometheus / micrometer requires all meters with the same name to have the
   * same tag keys, so all tags are used even for features not used by the request.
   *
   * <p>Not a Micrometer change — the check is identical in 1.14.7 and 1.17.0. Quarkus added the
   * throw: 3.38.1 vs 3.30.8 Before that it was a silent no-op
   * (L1240)[https://github.com/micrometer-metrics/micrometer/blob/v1.14.7/micrometer-core/src/main/java/io/micrometer/core/instrument/MeterRegistry.java#L1240-L1244]
   * , so these metrics were being dropped since #2058, not just now.
   *
   * @return A {@link Tags} object containing a tag for each feature.
   */
  public Tags getTags() {
    return Tags.of(
        ALL_FEATURES.stream()
            .map(f -> Tag.of(f.getTagName(), String.valueOf(commandFeatures.contains(f))))
            .toArray(Tag[]::new));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CommandFeatures that = (CommandFeatures) obj;
    return Objects.equals(commandFeatures, that.commandFeatures);
  }

  @Override
  public int hashCode() {
    return commandFeatures.hashCode();
  }

  @Override
  public String toString() {
    // CommandFeatures[features…]
    return "CommandFeatures" + commandFeatures.toString();
  }
}
