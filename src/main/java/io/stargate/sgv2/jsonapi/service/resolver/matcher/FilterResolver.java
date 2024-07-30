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
import io.stargate.sgv2.jsonapi.service.resolver.ClauseResolver;

import java.util.Objects;

/**
 * Base for classes that resolve a filter clause on a {@link Command} into something the Operation can
 * use to filter documents or rows.
 * <p>
 * TODO: this is a base for collections and table filters, currently collection uses the LogicalExpression
 * incorrectly. When we fix that this can return a {@link io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause} for all cases.
 * <p>
 * @param <CmdT> The type od the {@link Command} that is being resolved.
 * @param <SchemaT> The typ of the {@link SchemaObject} that {@link Command} command is operating on.
 */
public abstract class FilterResolver<
        CmdT extends Command & Filterable, SchemaT extends SchemaObject>
    extends ClauseResolver<CmdT, SchemaT, LogicalExpression> {

  protected final FilterMatchRules<CmdT> matchRules;

  protected FilterResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);

    matchRules = Objects.requireNonNull(buildMatchRules(), "buildMatchRules() must return non null");
    if (matchRules.isEmpty()){
      throw new IllegalArgumentException("buildMatchRules() must return non empty FilterMatchRules");
    }
  }

  /**
   * Subclasses must return a non-null and non-empty set of {@link FilterMatchRules} configured to
   * match against the {@link io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause}
   * for the type of {@link SchemaObject} this resolver is for.
   *
   * @return {@link FilterMatchRules< CmdT >}
   */
  protected abstract FilterMatchRules<CmdT> buildMatchRules();

  /**
   * Users of the class should call this function to convert the filer on the command into a {@link
   * LogicalExpression}.
   *
   * @param commandContext
   * @param command
   * @return
   */
  public LogicalExpression resolve(CommandContext<SchemaT> commandContext, CmdT command) {
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
