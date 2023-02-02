package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonException;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Applies a series of {@link FilterMatchRule}'s to either create an {@link Operation}
 *
 * <p>T - The command type we are resolving against.
 */
public class FilterMatchRules<T extends Command & Filterable> {

  // use the interface rather than MatchRule class so the streaming works.
  private final List<BiFunction<CommandContext, T, Optional<ReadOperation>>> matchRules =
      new ArrayList<>();
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
      BiFunction<CommandContext, CaptureGroups<T>, ReadOperation> resolveFunction,
      FilterMatcher.MatchStrategy matchStrategy) {
    FilterMatchRule<T> rule =
        new FilterMatchRule<T>(new FilterMatcher<>(matchStrategy), resolveFunction);
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
  public ReadOperation apply(CommandContext commandContext, T command) {
    return matchRules.stream()
        .map(e -> e.apply(commandContext, command))
        .filter(Optional::isPresent)
        .map(Optional::get) // unwraps the Optional from the resolver function.
        .findFirst()
        .orElseThrow(
            () ->
                new JsonException(
                    ErrorCode.FILTER_UNRESOLVABLE,
                    "Filter type not supported, unable to resolve to a filtering strategy"));
  }

  @VisibleForTesting
  protected List<BiFunction<CommandContext, T, Optional<ReadOperation>>> getMatchRules() {
    return matchRules;
  }
}
