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
  private final EnumSet<CommandFeature> commandFeatures;

  /** A instance representing no commandFeatures in use. */
  public static final CommandFeatures EMPTY =
      new CommandFeatures(EnumSet.noneOf(CommandFeature.class));

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
    this.commandFeatures.add(commandFeature);
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
      this.commandFeatures.addAll(other.commandFeatures);
    }
  }

  /**
   * Checks if this instance contains any features.
   *
   * @return {@code true} if no features are present, {@code false} otherwise.
   */
  public boolean isEmpty() {
    return this.commandFeatures.isEmpty();
  }

  /**
   * Gets a copy of the set of commandFeatures contained in this {@code CommandFeatures}. The
   * returned set is a defensive copy and modifications to it will not affect this {@code
   * CommandFeatures} instance.
   *
   * @return An {@link EnumSet} containing the commandFeatures.
   */
  public EnumSet<CommandFeature> getFeatures() {
    return EnumSet.copyOf(commandFeatures);
  }

  /**
   * Generates Micrometer Tags representing the features used. For every possible {@link
   * CommandFeature}, a tag is generated with the feature's tagName and a value of "true" if the
   * feature is present in this set, or "false" otherwise.
   *
   * @return A {@link Tags} object suitable for use with Micrometer metrics.
   */
  public Tags getAsTags() {
    Tags tags = Tags.empty();
    for (CommandFeature feature : CommandFeature.values()) {
      // Check presence directly against the internal set
      boolean isFeaturePresent = this.commandFeatures.contains(feature);
      tags = tags.and(Tag.of(feature.getTagName(), String.valueOf(isFeaturePresent)));
    }
    return tags;
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
    return "CommandFeatures" + commandFeatures.toString();
  }
}
