package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import io.quarkus.logging.Log;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.DefaultSubConditionRelation;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedSelect;
import io.stargate.sgv2.jsonapi.service.operation.query.*;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * Builds the WHERE clause in a CQL statement when using the Java Driver Query Builder.
 *
 * <p>NOTE: Using a class so the ctor can be made private to force use fo the static factories that
 * solve the generic typing needed for the {@link OngoingWhereClause}.
 *
 * @param <T> The type of Query Builder statement that the where clause is being added to, use the
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
  public static TableWhereCQLClause<Select> forSelect(
      TableSchemaObject table, DBLogicalExpression dbLogicalExpression) {
    return new TableWhereCQLClause<>(table, dbLogicalExpression);
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
  public static TableWhereCQLClause<Update> forUpdate(
      TableSchemaObject table, DBLogicalExpression dbLogicalExpression) {
    return new TableWhereCQLClause<>(table, dbLogicalExpression);
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

  /**
   * Apply the TableWhereCQLClause to the OnGoingWhereClause.
   *
   * <p>tOngoingWhereClause is {@link ExtendedSelect} applied from {@link SelectCQLClause}.
   *
   * <p>This method should iterate though the dbLogicalExpression, apply each logicalRelation to the
   * tOngoingWhereClause and apply each TableFilter to the tOngoingWhereClause
   *
   * @param tOngoingWhereClause the first function argument
   * @param objects the second function argument
   * @return
   */
  @Override
  public T apply(T tOngoingWhereClause, List<Object> objects) {
    Log.error("here " + dbLogicalExpression);

    // If API filter is empty, no where clause.
    if (dbLogicalExpression.isEmpty()) {
      return tOngoingWhereClause;
    }

    return tOngoingWhereClause.where(applyLogicalRelation(dbLogicalExpression, objects));
  }

  /**
   * This method will recursively resolve the DBLogicalExpression into two kinds of where Relation
   * that Driver QueryBuilder expects.
   *
   * <p>1. TableFilter -> Driver DefaultRelation 2. DBLogicalExpression(AND/OR) -> Driver Logical
   * Relation
   *
   * <p>Positional values are appended as usual in the flow, and the order matters.
   *
   * @param currentLogicalExpression currentLogicalExpression
   * @param objects positionalValues
   * @return Relation
   */
  private Relation applyLogicalRelation(
      DBLogicalExpression currentLogicalExpression, List<Object> objects) {
    var relationWhere = DefaultSubConditionRelation.subCondition();

    // 1. relations from TableFilters in this level)
    // 2. relations from logicalExpression in next level)
    List<Relation> combinedRelations =
        Stream.concat(
                currentLogicalExpression.filters().stream()
                    .map(filter -> ((TableFilter) filter).apply(tableSchemaObject, objects)),
                currentLogicalExpression.subExpressions().stream()
                    .map(subExpression -> applyLogicalRelation(subExpression, objects)))
            .toList();

    // construct and() relation
    if (currentLogicalExpression.operator() == DBLogicalExpression.DBLogicalOperator.AND
        && !combinedRelations.isEmpty()) {
      // and() together
      relationWhere = relationWhere.where(combinedRelations.getFirst());
      for (int i = 1; i < combinedRelations.size(); i++) {
        relationWhere = relationWhere.and().where(combinedRelations.get(i));
      }
    }

    // construct or() relation
    if (currentLogicalExpression.operator() == DBLogicalExpression.DBLogicalOperator.OR
        && !combinedRelations.isEmpty()) {
      // or() together
      relationWhere = relationWhere.where(combinedRelations.getFirst());
      for (int i = 1; i < combinedRelations.size(); i++) {
        relationWhere = relationWhere.or().where(combinedRelations.get(i));
      }
    }

    return relationWhere;
  }
}
