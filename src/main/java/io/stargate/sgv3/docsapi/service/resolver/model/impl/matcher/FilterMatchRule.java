package io.stargate.sgv3.docsapi.service.resolver.model.impl.matcher;

import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.Filterable;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * A single rule that if it matches a {@link Filterable} command to create an {@link Operation}
 *
 * <p>Use with the {@link FilterMatchRules}, expected to created via that class. resolveFunction
 * function to call if the matcher matches. Is only called if the {@link #matcher} matches
 * everything and returns a {@link CaptureGroups}
 *
 * <p>T - Command type to match
 */
public record FilterMatchRule<T extends Command & Filterable>(
    FilterMatcher<T> matcher,
    BiFunction<CommandContext, CaptureGroups<T>, Operation> resolveFunction)
    implements BiFunction<CommandContext, T, Optional<Operation>> {
  @Override
  public Optional<Operation> apply(CommandContext commandContext, T command) {
    return matcher.apply(command).map(captures -> resolveFunction.apply(commandContext, captures));
  }
}
