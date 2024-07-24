package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.ValidatableCommandClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/**
 * Base for classes that turn a configured {@link
 * io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause} inot a {@link
 * LogicalExpression} using the configured {@link FilterMatchRules}.
 *
 * @param <T> The type od the {@link Command} that is being resolved.
 * @param <U> The typ of the {@link SchemaObject} that {@link Command} command is operating on.
 */
public abstract class FilterResolver<T extends Command & Filterable, U extends SchemaObject> {

  protected final OperationsConfig operationsConfig;
  protected final FilterMatchRules<T> matchRules;

  protected FilterResolver(OperationsConfig operationsConfig) {
    Preconditions.checkNotNull(operationsConfig, "operationsConfig is required");
    this.operationsConfig = operationsConfig;

    matchRules = buildMatchRules();
    Preconditions.checkNotNull(matchRules, "buildMatchRules() must return non null");
    Preconditions.checkArgument(
        !matchRules.isEmpty(), "buildMatchRules() must return non empty FilterMatchRules");
  }

  /**
   * Subclasses must return a non-null and non-empty set of {@link FilterMatchRules} configured to
   * match against the {@link io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause}
   * for the type of {@link SchemaObject} this resolver is for.
   *
   * @return {@link FilterMatchRules<T>}
   */
  protected abstract FilterMatchRules<T> buildMatchRules();

  /**
   * Users of the class should call this function to convert the filer on the command into a {@link
   * LogicalExpression}.
   *
   * @param commandContext
   * @param command
   * @return
   */
  public LogicalExpression resolve(CommandContext<U> commandContext, T command) {
    Preconditions.checkNotNull(commandContext, "commandContext is required");
    Preconditions.checkNotNull(command, "command is required");

    ValidatableCommandClause.maybeValidate(commandContext, command.filterClause());

    LogicalExpression filter = matchRules.apply(commandContext, command);
    if (filter.getTotalComparisonExpressionCount() > operationsConfig.maxFilterObjectProperties()) {
      throw new JsonApiException(
          ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION,
          String.format(
              "%s: filter has %d fields, exceeds maximum allowed %s",
              ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION.getMessage(),
              filter.getTotalComparisonExpressionCount(),
              operationsConfig.maxFilterObjectProperties()));
    }
    return filter;
  }
}
