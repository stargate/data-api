package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.WhereCQLClause;
import java.util.List;
import java.util.Objects;

/**
 * Builds the WHERE clause in a CQL statment when using the Java Driver Query Builder.
 *
 * <p>TODO: this accepts the {@link LogicalExpression} to build the statement, we want to stop
 * handing that down to the operations but keeping for now for POC work.
 *
 * <p>NOTE: Using a class so the ctor can be made private to force use fo the static factories that
 * solve the generic typing needed for the {@link OngoingWhereClause}.
 *
 * @param <T> The type of Query Builder stament that the where clause is being added to, use the
 *     static factory methods like {@link #forSelect(TableSchemaObject, LogicalExpression)} to get
 *     the correct type.
 */
public class TableWhereCQLClause<T extends OngoingWhereClause<T>> implements WhereCQLClause<T> {

  private final TableSchemaObject table;
  private final LogicalExpression logicalExpression;

  private TableWhereCQLClause(TableSchemaObject table, LogicalExpression logicalExpression) {
    this.table = Objects.requireNonNull(table, "table must not be null");
    this.logicalExpression =
        Objects.requireNonNull(logicalExpression, "logicalExpression must not be null");
  }

  /**
   * Build an instance to add the where clause to a {@link Select}.
   *
   * <p>
   *
   * @param table
   * @param logicalExpression
   * @return
   */
  public static TableWhereCQLClause<Select> forSelect(
      TableSchemaObject table, LogicalExpression logicalExpression) {
    return new TableWhereCQLClause<>(table, logicalExpression);
  }

  /**
   * Build an instance to add the where clause to a {@link Update}.
   *
   * <p>
   *
   * @param table
   * @param logicalExpression
   * @return
   */
  public static TableWhereCQLClause<Update> forUpdate(
      TableSchemaObject table, LogicalExpression logicalExpression) {
    return new TableWhereCQLClause<>(table, logicalExpression);
  }

  @Override
  public T apply(T tOngoingWhereClause, List<Object> objects) {
    // TODO BUG: this probably break order for nested expressions, for now enough to get this tested
    var tableFilters =
        logicalExpression.comparisonExpressions.stream()
            .flatMap(comparisonExpression -> comparisonExpression.getDbFilters().stream())
            .map(dbFilter -> (TableFilter) dbFilter)
            .toList();

    // Add the where clause operations
    for (TableFilter tableFilter : tableFilters) {
      tOngoingWhereClause = tableFilter.apply(table, tOngoingWhereClause, objects);
    }
    return tOngoingWhereClause;
  }
}
