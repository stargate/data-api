package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.exception.WithWarnings;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.DefaultSubConditionRelation;
import io.stargate.sgv2.jsonapi.service.operation.query.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builds the WHERE clause in a CQL statement when using the Java Driver Query Builder.
 *
 * <p>TODO: this accepts the {@link LogicalExpression} to build the statement, we want to stop
 * handing that down to the operations but keeping for now for POC work.
 *
 * <p>NOTE: Using a class so the ctor can be made private to force use fo the static factories that
 * solve the generic typing needed for the {@link OngoingWhereClause}.
 *
 * @param <T> The type of Query Builder statement that the where clause is being added to, use the
 *     static factory methods like {@link #forSelect(TableSchemaObject, WithWarnings)} to get the
 *     correct type.
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
   * @param table the target table schema.
   * @param dbLogicalExpression the DB LogicalExpression that contains all the filters and
   *     logicalOperator
   */
  public static WithWarnings<TableWhereCQLClause<Select>> forSelect(
      TableSchemaObject table, WithWarnings<DBLogicalExpression> dbLogicalExpression) {
    return WithWarnings.of(
        new TableWhereCQLClause<>(table, dbLogicalExpression.target()), dbLogicalExpression);
  }

  /**
   * Build an instance to add the where clause to a {@link Update}.
   *
   * @param table the target table schema.
   * @param dbLogicalExpression the DB LogicalExpression that contains all the filters and
   *     logicalOperator
   */
  public static WithWarnings<TableWhereCQLClause<Update>> forUpdate(
      TableSchemaObject table, WithWarnings<DBLogicalExpression> dbLogicalExpression) {
    return WithWarnings.of(
        new TableWhereCQLClause<>(table, dbLogicalExpression.target()), dbLogicalExpression);
  }

  /**
   * Build an instance to add the where clause to a {@link Delete}.
   *
   * @param table the target table schema.
   * @param dbLogicalExpression the DB LogicalExpression that contains all the filters and
   *     logicalOperator
   */
  public static TableWhereCQLClause<Delete> forDelete(
      TableSchemaObject table, DBLogicalExpression dbLogicalExpression) {
    return new TableWhereCQLClause<>(table, dbLogicalExpression);
  }

  /**
   * Apply the {@link TableWhereCQLClause} to the {@link OngoingWhereClause}. It will recursively
   * apply all the filters and logical relations to build the {@link Relation} and feed to the
   * {@link OngoingWhereClause}.
   *
   * @param tOngoingWhereClause the {@link OngoingWhereClause} to apply the filters and logical
   *     relations to.
   * @param objects the positional values to append to the {@link Relation}.
   * @return the {@link OngoingWhereClause} with the filters and logical relations applied.
   */
  @Override
  public T apply(T tOngoingWhereClause, List<Object> objects) {

    // If there is no filter in the entire logicalExpression tree
    // Just return the ongoingWhereClause without apply anything.
    // This could happen when user provides no filter or empty filter in the request.
    if (dbLogicalExpression.isEmpty()) {
      return tOngoingWhereClause;
    }

    return tOngoingWhereClause.where(applyLogicalRelation(dbLogicalExpression, objects));
  }

  /**
   * Method to recursively resolve the DBLogicalExpression into regular relation {@link Relation} or
   * AND/OR relation {@link DefaultSubConditionRelation} that Driver QueryBuilder expects.
   *
   * @param currentLogicalExpression currentLogicalExpression.
   * @param objects positionalValues to append in order.
   * @return {@link Relation} conjunct relation from DBLogicalExpression.
   */
  private Relation applyLogicalRelation(
      DBLogicalExpression currentLogicalExpression, List<Object> objects) {

    // create the default relation to represent the current level of AND/OR, E.G.
    // implicit and: {"name": "John"} -> WHERE (name=?)
    // implicit and with explicit or: {"$or": [{"name": "John"}, {"age": 30}]} -> WHERE ((name=? OR
    // age=?))
    // Ideally, we don't need the parenthesis in the root level
    // but this is how the driver override works current, see DefaultSubConditionRelation.java
    // We can not remove the root level parenthesis currently to build the logical relation
    var relationWhere = DefaultSubConditionRelation.subCondition();

    List<Relation> relations = new ArrayList<>();
    // First, add all simple filters
    currentLogicalExpression
        .filters()
        .forEach(filter -> relations.add(((TableFilter) filter).apply(tableSchemaObject, objects)));

    // Then, recursively build relations from sub_levels
    currentLogicalExpression
        .subExpressions()
        .forEach(subExpr -> relations.add(applyLogicalRelation(subExpr, objects)));

    // if current logical operator is AND, construct and() relation
    if (currentLogicalExpression.operator() == DBLogicalExpression.DBLogicalOperator.AND
        && !relations.isEmpty()) {
      relationWhere = relationWhere.where(relations.getFirst());
      for (int i = 1; i < relations.size(); i++) {
        relationWhere = relationWhere.and().where(relations.get(i));
      }
    }

    // if current logical operator is AND, construct and() relation
    if (currentLogicalExpression.operator() == DBLogicalExpression.DBLogicalOperator.OR
        && !relations.isEmpty()) {
      relationWhere = relationWhere.where(relations.getFirst());
      for (int i = 1; i < relations.size(); i++) {
        relationWhere = relationWhere.or().where(relations.get(i));
      }
    }

    return relationWhere;
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

  @Override
  public DBLogicalExpression getLogicalExpression() {
    return dbLogicalExpression;
  }
}
