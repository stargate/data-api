package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.service.operation.Operation;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
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
    BiFunction<DBLogicalExpression, CaptureGroups<T>, DBLogicalExpression> resolveFunction)
    implements BiFunction<CommandContext, T, Optional<DBLogicalExpression>> {

  @Override
  public Optional<DBLogicalExpression> apply(CommandContext commandContext, T command) {
    return matcher
        .apply(commandContext, command)
        .map(
            captures ->
                resolveFunction.apply(
                    new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND), captures));
  }
}
