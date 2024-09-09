package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterLogicalExpression;
import io.stargate.sgv2.jsonapi.service.resolver.ClauseResolver;
import java.util.Objects;

/**
 * Base for classes that resolve a filter clause on a {@link Command} into something the Operation
 * can use to filter documents or rows.
 *
 * <p>TODO: this is a base for collections and table filters, currently collection uses the
 * LogicalExpression incorrectly. When we fix that this can return a {@link
 * io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause} for all cases.
 *
 * <p>
 *
 * @param <CmdT> The type of the {@link Command} that is being resolved.
 * @param <SchemaT> The type of the {@link SchemaObject} that {@link Command} command is operating
 *     on.
 */
public abstract class FilterResolver<
        CmdT extends Command & Filterable, SchemaT extends SchemaObject>
    extends ClauseResolver<CmdT, SchemaT, DBFilterLogicalExpression> {

  protected final FilterMatchRules<CmdT> matchRules;

  protected FilterResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);

    matchRules =
        Objects.requireNonNull(buildMatchRules(), "buildMatchRules() must return non null");
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
   * DBFilterLogicalExpression}.
   *
   * @param commandContext
   * @param command
   * @return DBFilterLogicalExpression
   */
  public DBFilterLogicalExpression resolve(CommandContext<SchemaT> commandContext, CmdT command) {
    Preconditions.checkNotNull(commandContext, "commandContext is required");
    Preconditions.checkNotNull(command, "command is required");

    ValidatableCommandClause.maybeValidate(commandContext, command.filterClause());
    InvertibleCommandClause.maybeInvert(commandContext, command.filterClause());

    final DBFilterLogicalExpression dbFilterLogicalExpression =
        matchRules.apply(commandContext, command);

    // TODO, why validate here?
    if (command.filterClause() != null
        && command.filterClause().logicalExpression().getTotalComparisonExpressionCount()
            > operationsConfig.maxFilterObjectProperties()) {
      throw new JsonApiException(
          ErrorCodeV1.FILTER_FIELDS_LIMIT_VIOLATION,
          String.format(
              "%s: filter has %d fields, exceeds maximum allowed %s",
              ErrorCodeV1.FILTER_FIELDS_LIMIT_VIOLATION.getMessage(),
              command.filterClause().logicalExpression().getTotalComparisonExpressionCount(),
              operationsConfig.maxFilterObjectProperties()));
    }
    return dbFilterLogicalExpression;
  }
}
