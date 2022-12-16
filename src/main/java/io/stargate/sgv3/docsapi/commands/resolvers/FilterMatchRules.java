package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.clauses.FilterClause.Filterable;
import io.stargate.sgv3.docsapi.operations.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Applies a series of {@link FilterMatchRule}'s to either create an {@link Operation} or throw
 * {@link CommandResolver.UnresolvedCommandException} when no match.
 *
 * <p>T - The command type we are resolving against.
 */
class FilterMatchRules<T extends Command & Filterable> {

  // use the interface rather than MatchRule class so the streaming works.
  private final List<BiFunction<CommandContext, T, Optional<Operation>>> matchRules =
      new ArrayList<>();
  private final List<BiFunction<CommandContext, T, Optional<Operation>>> emptyRules =
      new ArrayList<>();

  FilterMatchRules() {}

  /**
   * Adds a rule that will result in the specified resolveFunction being called.
   *
   * <p>Rules are applied in the order they are added, so add most specific first.
   *
   * <p>Caller should then configure the rule as they want, e.g. <code>
   *      private final FilterMatchRules<FindOneCommand> matchRules = new FilterMatchRules<>();
   *      matchRules.addMatchRule(FindOneCommandResolver::findById).matcher
   *        .capture(ID_GROUP).eq("_id", JsonType.STRING);
   *      ...
   *      private static Operation findById(CommandContext commandContext, Captures<FindOneCommand> captures){
   *          CaptureGroup captureGroup = captures.getCapture(ID_GROUP);
   *          return new FindByIdOperation(commandContext, captureGroup.getSingleJsonLiteral().getTypedValue());
   *      }
   * </code>
   *
   * @param resolveFunction
   * @return
   */
  public FilterMatchRule<T> addMatchRule(
      BiFunction<CommandContext, CaptureGroups<T>, Operation> resolveFunction,
      FilterMatcher.MatchStrategy matchStrategy) {
    FilterMatchRule<T> rule = new FilterMatchRule<T>(resolveFunction, matchStrategy);
    matchRules.add(rule);
    return rule;
  }

  /**
   * Applies all the rules to to return an Operation or throw.
   *
   * @param commandContext
   * @param command
   * @return
   */
  public Operation apply(CommandContext commandContext, T command) {
    return matchRules.stream()
        .map(e -> e.apply(commandContext, command))
        .filter(Optional::isPresent)
        .map(Optional::get) // unwraps the Optional from the resolver function.
        .findFirst()
        .orElseThrow(
            () -> {
              return CommandResolver.getUnresolvedCommandException(command);
            });
  }
}
