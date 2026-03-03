package io.stargate.sgv2.jsonapi.service.resolver.matcher;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.FilterClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.JsonType;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
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
    extends ClauseResolver<CmdT, SchemaT, DBLogicalExpression> {

  protected final FilterMatchRules<CmdT> matchRules;

  protected FilterResolver(OperationsConfig operationsConfig) {
    super(operationsConfig);

    matchRules =
        Objects.requireNonNull(buildMatchRules(), "buildMatchRules() must return non null");
    if (matchRules.isEmpty()) {
      throw new IllegalArgumentException(
          "buildMatchRules() must return non empty FilterMatchRules");
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
   * Users of the class should call this function to convert the filter on the command into a {@link
   * DBLogicalExpression}.
   *
   * @param commandContext
   * @param command
   * @return DBLogicalExpression
   */
  public WithWarnings<DBLogicalExpression> resolve(
      CommandContext<SchemaT> commandContext, CmdT command) {
    Preconditions.checkNotNull(commandContext, "commandContext is required");
    Preconditions.checkNotNull(command, "command is required");

    try {
      final DBLogicalExpression dbLogicalExpression = matchRules.apply(commandContext, command);
      return WithWarnings.of(dbLogicalExpression);
    } catch (ServerException e) {
      if (commandContext.schemaObject() instanceof TableSchemaObject tableSchemaObject) {
        throw buildTableFilterError(commandContext, command, tableSchemaObject, e);
      }
      throw e;
    }
  }

  /**
   * When a table filter fails to match any rule, inspect the filter to find columns with
   * incompatible value types (e.g. SUB_DOC for a primitive column) and throw a descriptive {@link
   * FilterException} instead of a generic server error.
   */
  private FilterException buildTableFilterError(
      CommandContext<SchemaT> commandContext,
      CmdT command,
      TableSchemaObject tableSchemaObject,
      ServerException cause) {
    FilterClause filterClause = command.filterClause(commandContext);
    if (filterClause != null && !filterClause.isEmpty()) {
      for (ComparisonExpression expr : filterClause.logicalExpression().comparisonExpressions) {
        boolean hasSubDoc =
            expr.getFilterOperations().stream()
                .anyMatch(op -> op.operand() != null && op.operand().type() == JsonType.SUB_DOC);
        if (hasSubDoc) {
          String path = expr.getPath();
          var columnOpt = tableSchemaObject.tableMetadata().getColumn(path);
          String columnType = columnOpt.map(col -> col.getType().toString()).orElse("unknown");
          throw FilterException.Code.INVALID_FILTER_COLUMN_VALUES.get(
              errVars(
                  tableSchemaObject,
                  m -> {
                    m.put(
                        "allColumns",
                        errFmtColumnMetadata(
                            tableSchemaObject.tableMetadata().getColumns().values()));
                    m.put("invalidColumn", path);
                    m.put("columnType", columnType);
                  }));
        }
      }
    }
    // If we couldn't identify a specific problematic column, re-throw original
    throw cause;
  }
}
