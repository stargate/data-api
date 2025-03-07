package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.*;
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
 *     static factory methods like {@link #forSelect(TableSchemaObject, DBLogicalExpression)} to get
 *     the correct type.
 */
public class TableWhereCQLClause<T extends OngoingWhereClause<T>> implements WhereCQLClause<T> {

  private final TableSchemaObject tableSchemaObject;
  private final DBLogicalExpression dbLogicalExpression;

  private TableWhereCQLClause(
      TableSchemaObject tableSchemaObject, DBLogicalExpression dbLogicalExpression) {
    this.tableSchemaObject = Objects.requireNonNull(tableSchemaObject, "table must not be null");
    this.dbLogicalExpression =
        Objects.requireNonNull(dbLogicalExpression, "logicalExpression must not be null");
  }

  /**
   * Build an instance to add the where clause to a {@link Select}.
   *
   * <p>
   *
   * @param table
   * @param dbLogicalExpression
   * @return
   */
  public static WithWarnings<TableWhereCQLClause<Select>> forSelect(
      TableSchemaObject table, WithWarnings<DBLogicalExpression> dbLogicalExpression) {
    return WithWarnings.of(
        new TableWhereCQLClause<>(table, dbLogicalExpression.target()), dbLogicalExpression);
  }

  /**
   * Build an instance to add the where clause to a {@link Update}.
   *
   * <p>
   *
   * @param table
   * @param dbLogicalExpression
   * @return
   */
  public static WithWarnings<TableWhereCQLClause<Update>> forUpdate(
      TableSchemaObject table, WithWarnings<DBLogicalExpression> dbLogicalExpression) {
    return WithWarnings.of(
        new TableWhereCQLClause<>(table, dbLogicalExpression.target()), dbLogicalExpression);
  }

  /**
   * Build an instance to add the where clause to a {@link Delete}.
   *
   * <p>
   *
   * @param table
   * @param dbLogicalExpression
   * @return
   */
  public static TableWhereCQLClause<Delete> forDelete(
      TableSchemaObject table, DBLogicalExpression dbLogicalExpression) {
    return new TableWhereCQLClause<>(table, dbLogicalExpression);
  }

  @Override
  public DBLogicalExpression getLogicalExpression() {
    return dbLogicalExpression;
  }

  @Override
  public T apply(T tOngoingWhereClause, List<Object> objects) {
    // TODO BUG: this probably breaks order for nested expressions, for now enough to get this
    // tested
    var tableFilters =
        dbLogicalExpression.filters().stream().map(dbFilter -> (TableFilter) dbFilter).toList();

    // Add the where clause operations
    for (TableFilter tableFilter : tableFilters) {
      tOngoingWhereClause = tableFilter.apply(tableSchemaObject, tOngoingWhereClause, objects);
    }
    return tOngoingWhereClause;
  }

  @Override
  public boolean selectsSinglePartition(TableSchemaObject tableSchemaObject) {

    var apiTableDef =
        Objects.requireNonNull(tableSchemaObject, "tableSchemaObject must not be null")
            .apiTableDef();

    final boolean[] isMatched = {false};
    for (var apiColumnDef : apiTableDef.partitionKeys().values()) {
      isMatched[0] = false;
      dbLogicalExpression.visitAllFilters(
          TableFilter.class,
          tableFilter ->
              isMatched[0] =
                  isMatched[0]
                      || tableFilter.isFor(apiColumnDef.name())
                          && tableFilter.filterIsExactMatch());
      // we only need to find one partition column that does not have an exact match filter on it
      if (!isMatched[0]) {
        return false;
      }
    }
    return true;
  }
}
