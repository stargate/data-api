package io.stargate.sgv2.jsonapi.service.operation.query;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.UNSUPPORTED_FILTER_OPERATION;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import java.util.ArrayList;
import java.util.List;

public class DBFilterLogicalExpression {

  /**
   * This is another enum class for LogicalOperator Different with LogicalOperator in
   * LogicalExpression This one does not have the NOT operator and not invertible Basically made
   * just for preserve logical relation
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

    public static DBLogicalOperator convertFromLogicalOperator(
        LogicalExpression.LogicalOperator logicalOperator) {
      return switch (logicalOperator) {
        case AND -> DBLogicalOperator.AND;
        case OR -> DBLogicalOperator.OR;
        default ->
            throw UNSUPPORTED_FILTER_OPERATION.toApiException(
                "convert from logical operator failure, unsupported operator: " + logicalOperator);
      };
    }
  }

  private final DBLogicalOperator dbLogicalOperator;

  private final List<DBFilterBase> dbFilterList;

  private final List<DBFilterLogicalExpression> dbFilterLogicalExpressionList;

  public DBFilterLogicalExpression(DBLogicalOperator dbLogicalOperator) {
    this.dbLogicalOperator = dbLogicalOperator;
    this.dbFilterList = new ArrayList<>();
    this.dbFilterLogicalExpressionList = new ArrayList<>();
  }

  /**
   * add a logical relation of DBFilterLogicalExpression to the dbFilterLogicalExpressionList
   *
   * @param DBFilterLogicalExpression innerDBFilterLogicalExpression
   * @return DBFilterLogicalExpression
   */
  public DBFilterLogicalExpression addDBFilterLogicalExpression(
      DBFilterLogicalExpression innerDBFilterLogicalExpression) {
    dbFilterLogicalExpressionList.add(innerDBFilterLogicalExpression);
    return innerDBFilterLogicalExpression;
  }

  /**
   * add a DBFilterBase to the dbFilterList
   *
   * @param DBFilterBase innerDBFilter
   * @return DBFilterBase
   */
  public DBFilterBase addInnerDBFilter(DBFilterBase innerDBFilter) {
    dbFilterList.add(innerDBFilter);
    return innerDBFilter;
  }

  public DBLogicalOperator getDbLogicalOperator() {
    return dbLogicalOperator;
  }

  public boolean isEmpty() {
    return dbFilterLogicalExpressionList.isEmpty() && dbFilterList.isEmpty();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DBFilterLogicalExpression{");
    sb.append("dbLogicalOperator='").append(dbLogicalOperator).append("'");
    sb.append(", dbFilterLogicalExpressionList=").append(dbFilterLogicalExpressionList);
    sb.append(", dbFilterList=").append(dbFilterList);
    sb.append("}");
    return sb.toString();
  }

  public List<DBFilterBase> getDbFilterList() {
    return dbFilterList;
  }

  public List<DBFilterLogicalExpression> getDbFilterLogicalExpressionList() {
    return dbFilterLogicalExpressionList;
  }

  public int getTotalDbFiltersAmount() {
    return getTotalDbFiltersAmountHelper(this);
  }

  private int getTotalDbFiltersAmountHelper(DBFilterLogicalExpression dbFilterLogicalExpression) {
    return dbFilterLogicalExpression.getDbFilterList().size()
        + dbFilterLogicalExpression.getDbFilterLogicalExpressionList().stream()
            .mapToInt(this::getTotalDbFiltersAmountHelper)
            .sum();
  }
}
