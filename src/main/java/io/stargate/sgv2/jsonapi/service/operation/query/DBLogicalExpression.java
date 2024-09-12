package io.stargate.sgv2.jsonapi.service.operation.query;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.ArrayList;
import java.util.List;

/**
 * DBLogicalExpression is what we use in DB level. (LogicalExpression in API level) Similarly to
 * LogicalExpression, this class also has recursive definition by having logical relation context.
 *
 * <p>fields dbLogicalExpressionList and dbFilterList are mutable, because we need to construct the
 * DBLogicalExpression recursively by adding new dbFilters and subDbLogicalExpression.
 */
public class DBLogicalExpression {

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

  private final List<DBFilterBase> dbFilters = new ArrayList<>();

  private final List<DBLogicalExpression> dbLogicalExpressions = new ArrayList<>();

  public DBLogicalExpression(DBLogicalOperator dbLogicalOperator) {
    this.dbLogicalOperator = dbLogicalOperator;
  }

  /**
   * Add a sub dbLogicalExpression as subExpression to current caller dbLogicalExpression
   *
   * @param DBLogicalExpression subDBLogicalExpression
   * @return subDBLogicalExpression
   */
  public DBLogicalExpression addDBLogicalExpression(DBLogicalExpression subDBLogicalExpression) {
    dbLogicalExpressions.add(subDBLogicalExpression);
    return subDBLogicalExpression;
  }

  /**
   * Add a dbFilter to current caller dbLogicalExpression. This new DBFilter will be added in the
   * dbFilter List, it will be in the relation context of this dbLogicalExpression.
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
    return dbLogicalExpressions.isEmpty() && dbFilters.isEmpty();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DBLogicalExpression{");
    sb.append("dbLogicalOperator='").append(dbLogicalOperator).append("'");
    sb.append(", dbLogicalExpressions=").append(dbLogicalExpressions);
    sb.append(", dbFilters=").append(dbFilters);
    sb.append("}");
    return sb.toString();
  }

  public List<DBFilterBase> dBFilters() {
    return List.copyOf(dbFilters);
  }

  public List<DBLogicalExpression> dbLogicalExpressions() {
    return List.copyOf(dbLogicalExpressions);
  }

  /**
   * Count dBFilters amount in DBLogicalExpression. This method will recursively sum up all sub
   * DBLogicalExpression.
   *
   * @return int
   */
  public int totalFilterCount() {
    var childCounts =
        dbLogicalExpressions().stream().mapToInt(DBLogicalExpression::totalFilterCount).sum();
    return childCounts + dbFilters.size();
  }
}
