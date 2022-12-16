package io.stargate.sgv3.docsapi.commands.resolvers;

import com.google.common.base.Functions;
import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.resolvers.CaptureGroup.CapturePair;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * All the {@link CaptureGroup}s we got from matching against a command.
 *
 * <p>This is "result" of running the FilterMatcher, and the value we pass to the resolver function
 * so it has raw command and all the captures. See {@link FilterMatchRule}
 *
 * <p>Each Capture you create from {@link FilterMatcher#capture(Object)} with a Marker is available
 * here as a {@link CaptureGroup} via {@link #getGroup(Object)}.
 *
 * <p>Created in the {@link FilterMatcher} via a builder
 *
 * <p>T - The {@link Command} that is filtered against
 */
public class CaptureGroups<T extends Command> {
  public final T command;

  // key is the {@link Capture.marker}
  private final Map<Object, CaptureGroup> captures;

  private CaptureGroups(T command, Map<Object, CaptureGroup> captures) {
    this.command = command;
    // assume the builder is handing in an unmodifiable map
    this.captures = captures;
  }

  /**
   * Get the {@link CaptureGroup} that has the result of the {@link Capture} created with the
   * supplied marker
   *
   * @param marker
   * @return CaptureGroup or throws
   */
  public Optional<CaptureGroup> getGroup(Object marker) {
    return Optional.ofNullable(captures.get(marker));
  }

  public void consumeAllCaptures(Object marker, Consumer<CapturePair> consumer) {
    getGroup(marker).ifPresent(group -> group.consumeAllCaptures(consumer));
  }

  static <T extends Command> Builder<T> builder(T command) {
    return new Builder<>(command);
  }

  static class Builder<T extends Command> {
    private Map<Object, CaptureGroup.Builder> groupBuilders = new HashMap<>();
    private final T command;

    public Builder(T command) {
      this.command = command;
    }

    protected CaptureGroup.Builder getCaptureGroupBuilder(Object marker) {
      return groupBuilders.computeIfAbsent(marker, CaptureGroup::builder);
    }

    protected CaptureGroups<T> build() {
      Map<Object, CaptureGroup> captures =
          groupBuilders.values().stream()
              .map(CaptureGroup.Builder::build)
              .collect(Collectors.toMap(CaptureGroup::getMarker, Functions.identity()));
      return new CaptureGroups<T>(command, captures);
    }
  }
}
