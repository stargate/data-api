package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
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
public record FilterMatchRule<T extends Command & Filterable>(FilterMatcher<T> matcher)
    implements BiFunction<CommandContext, T, Optional<LogicalExpression>> {
  @Override
  public Optional<LogicalExpression> apply(CommandContext commandContext, T command) {
    // 用当前FilterMatchRule 的 matcher 去 尝试capture, 得到的captureGroups 里面   private final Map<Object,
    // CaptureGroup<?>> groups; 然后CaptureGroup里 是 Map<String, List<FilterOperation<TYPE>>> captures

    return matcher.apply(command);
  }
}
