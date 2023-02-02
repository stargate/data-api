package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import java.util.HashMap;
import java.util.Map;

/**
 * All the {@link CaptureGroup}s we got from matching against a command.
 *
 * <p>This is "result" of running the FilterMatcher, and the value we pass to the resolver function
 * so it has raw command and all the groups. See
 *
 * <p>Each Capture you create from {@link FilterMatcher#capture(Object)} with a Marker is available
 * here as a {@link CaptureGroup} via {@link #getGroup(Object)}.
 *
 * <p>Created in the {@link FilterMatcher} via a builder
 *
 * <p>T - The {@link Command} that is filtered against
 */
public class CaptureGroups<T extends Command> {
  private final T command;
  private final Map<Object, CaptureGroup<?>> groups;

  public CaptureGroups(T command) {
    this.command = command;
    this.groups = new HashMap<>();
  }

  public T command() {
    return command;
  }

  /**
   * Get the {@link CaptureGroup} that has the result of the {@link FilterMatcher.Capture} created
   * with the supplied marker
   *
   * @param marker
   * @return CaptureGroup
   */
  public CaptureGroup<?> getGroup(Object marker) {
    return groups.computeIfAbsent(marker, f -> new CaptureGroup(new HashMap<>()));
  }

  /**
   * Get the {@link CaptureGroup} that has the result of the {@link FilterMatcher.Capture} created
   * with the supplied marker
   *
   * @param marker
   * @return CaptureGroup
   */
  public CaptureGroup<?> getGroupIfPresent(Object marker) {
    return groups.get(marker);
  }
}
