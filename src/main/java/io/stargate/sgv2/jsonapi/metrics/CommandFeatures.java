package io.stargate.sgv2.jsonapi.metrics;

import java.util.EnumSet;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a collection of {@link CommandFeature}s in a command. This class is immutable and can
 * be used to track which commandFeatures are utilized in a command. This class is immutable;
 * methods that modify the set of commandFeatures return a new {@code CommandFeatures} instance. It
 * uses an {@link EnumSet} internally for efficient storage and operations on commandFeatures.
 */
public final class CommandFeatures {
  private final EnumSet<CommandFeature> commandFeatures;

  /** A instance representing no commandFeatures in use. */
  public static final CommandFeatures EMPTY =
      new CommandFeatures(EnumSet.noneOf(CommandFeature.class));

  private CommandFeatures(EnumSet<CommandFeature> commandFeatures) {
    this.commandFeatures = EnumSet.copyOf(commandFeatures);
  }

  /**
   * Creates a {@code CommandFeatures} instance from an array of {@link CommandFeature}s.
   *
   * @param commandFeatures The commandFeatures to include. If null or empty, {@link #EMPTY} is
   *     returned.
   * @return A new {@code CommandFeatures} instance containing the specified commandFeatures.
   */
  public static CommandFeatures of(CommandFeature... commandFeatures) {
    if (commandFeatures == null || commandFeatures.length == 0) {
      return EMPTY;
    }
    return new CommandFeatures(EnumSet.of(commandFeatures[0], commandFeatures));
  }

  /**
   * Creates a {@code CommandFeatures} instance from an {@link EnumSet} of {@link CommandFeature}s.
   *
   * @param commandFeatures The set of commandFeatures to include. If null or empty, {@link #EMPTY}
   *     is returned.
   * @return A new {@code CommandFeatures} instance containing the specified commandFeatures.
   */
  public static CommandFeatures of(EnumSet<CommandFeature> commandFeatures) {
    if (commandFeatures == null || commandFeatures.isEmpty()) {
      return EMPTY;
    }
    return new CommandFeatures(commandFeatures);
  }

  /**
   * Returns a new {@code CommandFeatures} instance that includes the specified commandFeature in
   * addition to the commandFeatures in this instance. If the commandFeature is already present,
   * this instance is returned.
   *
   * @param commandFeature The commandFeature to add. Must not be null.
   * @return A new {@code CommandFeatures} instance with the added commandFeature, or this instance
   *     if the commandFeature was already present.
   * @throws NullPointerException if the commandFeature is null.
   */
  public CommandFeatures withFeature(CommandFeature commandFeature) {
    Objects.requireNonNull(commandFeature, "CommandFeature cannot be null");
    if (this.commandFeatures.contains(commandFeature)) {
      return this;
    }
    EnumSet<CommandFeature> newSet = EnumSet.copyOf(this.commandFeatures);
    newSet.add(commandFeature);
    return new CommandFeatures(newSet);
  }

  /**
   * Returns a new {@code CommandFeatures} instance that is the union of this instance's
   * commandFeatures and the commandFeatures of another {@code CommandFeatures} instance.
   *
   * @param other The other {@code CommandFeatures} instance to combine with. If null, empty, or the
   *     same as this instance, this instance is returned. If this instance is empty, the other
   *     instance is returned.
   * @return A new {@code CommandFeatures} instance representing the combined set of
   *     commandFeatures.
   */
  public CommandFeatures unionWith(CommandFeatures other) {
    if (other == null || other.commandFeatures.isEmpty() || other == this) {
      return this;
    }
    if (this.commandFeatures.isEmpty()) {
      return other;
    }
    EnumSet<CommandFeature> combined = EnumSet.copyOf(this.commandFeatures);
    combined.addAll(other.commandFeatures);
    return new CommandFeatures(combined);
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
    return "CommandFeatures{"
        + commandFeatures.stream().map(Enum::name).collect(Collectors.joining(", "))
        + '}';
  }
}
