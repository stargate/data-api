package io.stargate.sgv2.jsonapi.service.operation.query;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.ArrayList;
import java.util.List;

/**
 * DBFilterLogicalExpression is what we use in DB level. (LogicalExpression in API level) Similarly
 * to LogicalExpression, this class also has recursive definition by having logical relation
 * context.
 *
 * <p>fields dbFilterLogicalExpressionList and dbFilterList are mutable, because we need to
 * construct the DBFilterLogicalExpression recursively by adding new dbFilters and
 * subDbFilterLogicalExpression.
 */
public class DBFilterLogicalExpression {

  /**
   * This is another enum class for LogicalOperator. We have this one because LogicalOperator here
   * is associated with DB operations and not API. Different with LogicalOperator in
   * LogicalExpression. This one does not have the NOT operator(not supported by CQL) and not
   * invertible.
   */
  public enum DBLogicalOperator {
    AND("and"),
    OR("or");
    private final String operator;

    DBLogicalOperator(String operator) {
      this.operator = operator;
    }

    public String getOperator() {
      return operator;
    }

    public static DBLogicalOperator fromLogicalOperator(
        LogicalExpression.LogicalOperator logicalOperator) {
      return switch (logicalOperator) {
        case AND -> DBLogicalOperator.AND;
        case OR -> DBLogicalOperator.OR;
        default ->
            throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(
                "convert from logical operator failure, unsupported operator: " + logicalOperator);
      };
    }
  }

  private final DBLogicalOperator dbLogicalOperator;

  private final List<DBFilterBase> dbFilters;

  private final List<DBFilterLogicalExpression> dBFilterLogicalExpressions;

  public DBFilterLogicalExpression(DBLogicalOperator dbLogicalOperator) {
    this.dbLogicalOperator = dbLogicalOperator;
    this.dbFilters = new ArrayList<>();
    this.dBFilterLogicalExpressions = new ArrayList<>();
  }

  /**
   * Add a child dbFilterLogicalExpression as subExpression to current caller
   * dbFilterLogicalExpression
   *
   * @param DBFilterLogicalExpression innerDBFilterLogicalExpression
   * @return DBFilterLogicalExpression
   */
  public DBFilterLogicalExpression addDBFilterLogicalExpression(
      DBFilterLogicalExpression innerDBFilterLogicalExpression) {
    dBFilterLogicalExpressions.add(innerDBFilterLogicalExpression);
    return innerDBFilterLogicalExpression;
  }

  /**
   * Add a dbFilter to current caller dbFilterLogicalExpression. This new DBFilter will be added in
   * the dbFilter List, it will be in the relation context of this dbFilterLogicalExpression.
   *
   * @param DBFilterBase dBFilter
   * @return DBFilterBase
   */
  public DBFilterBase addDBFilter(DBFilterBase dBFilter) {
    dbFilters.add(dBFilter);
    return dBFilter;
  }

  public DBLogicalOperator operator() {
    return dbLogicalOperator;
  }

  public boolean isEmpty() {
    return dBFilterLogicalExpressions.isEmpty() && dbFilters.isEmpty();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DBFilterLogicalExpression{");
    sb.append("dbLogicalOperator='").append(dbLogicalOperator).append("'");
    sb.append(", dbFilterLogicalExpressionList=").append(dBFilterLogicalExpressions);
    sb.append(", dbFilterList=").append(dbFilters);
    sb.append("}");
    return sb.toString();
  }

  public List<DBFilterBase> dBFilters() {
    return List.copyOf(dbFilters);
  }

  public List<DBFilterLogicalExpression> dBFilterLogicalExpressions() {
    return List.copyOf(dBFilterLogicalExpressions);
  }

  /**
   * Count dBFilters amount in DBFilterLogicalExpression. This method will recursively sum up all
   * sub DBFilterLogicalExpression.
   *
   * @return int
   */
  public int totalFilterCount() {
    var childCounts =
        dBFilterLogicalExpressions().stream()
            .mapToInt(DBFilterLogicalExpression::totalFilterCount)
            .sum();
    return childCounts + dbFilters.size();
  }
}
