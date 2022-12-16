package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause.Filterable;
import io.stargate.sgv3.docsapi.operations.Operation;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * A single rule that if it matches a {@link Filterable} command to create an {@link Operation}
 *
 * <p>Use with the {@link FilterMatchRules}, expected to created via that class.
 *
 * <p>T - Command type to match
 */
class FilterMatchRule<T extends Command & Filterable>
    implements BiFunction<CommandContext, T, Optional<Operation>> {

  public final FilterMatcher<T> matcher;
  private BiFunction<CommandContext, CaptureGroups<T>, Operation> resolveFunction;

  /**
   * @param resolveFunction function to call if the matcher matches. Is only called if the {@link
   *     #matcher} matches everything and returns a {@link CaptureGroups}
   */
  FilterMatchRule(
      BiFunction<CommandContext, CaptureGroups<T>, Operation> resolveFunction,
      FilterMatcher.MatchStrategy matchStrategy) {
    this.resolveFunction = resolveFunction;
    this.matcher = new FilterMatcher<T>(matchStrategy);
  }

  @Override
  public Optional<Operation> apply(CommandContext commandContext, T command) {
    return matcher.apply(command).map(captures -> resolveFunction.apply(commandContext, captures));
  }
}
