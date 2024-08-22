package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import java.util.*;

/**
 * This object represents conditions based for a json path (node) that need to be tested Spec says
 * you can do this, compare equals to a literal {"username" : "aaron"} This is a shortcut for
 * {"username" : {"$eq" : "aaron"}} In here we expand the shortcut into a canonical long form, so it
 * is all the same.
 */
public class LogicalExpression {

  public enum LogicalOperator implements Invertible {
    AND("$and"),
    OR("$or"),
    NOT("$not");
    private String operator;

    LogicalOperator(String operator) {
      this.operator = operator;
    }

    public String getOperator() {
      return operator;
    }

    @Override
    public Invertible invert() {
      if (this == AND) {
        return OR;
      } else if (this == OR) {
        return AND;
      }
      return NOT;
    }
  }

  public final List<LogicalExpression> logicalExpressions;
  public final List<ComparisonExpression> comparisonExpressions;

  private LogicalOperator logicalRelation;

  // These two counters will sum up all the matched expression in the Expression tree. (Note: not
  // just current level)
  // TODO: the list is public so anyone can change it and break this count. May need to re-design
  private int totalComparisonExpressionCount;
  private int totalIdComparisonExpressionCount;

  private LogicalExpression(
      LogicalOperator logicalRelation,
      int totalComparisonExpressionCount,
      List<LogicalExpression> logicalExpressions,
      List<ComparisonExpression> comparisonExpression) {
    this.logicalRelation = logicalRelation;
    this.totalComparisonExpressionCount = totalComparisonExpressionCount;
    this.logicalExpressions = logicalExpressions;
    this.comparisonExpressions = comparisonExpression;
  }

  public static LogicalExpression and() {
    return new LogicalExpression(LogicalOperator.AND, 0, new ArrayList<>(), new ArrayList<>());
  }

  public static LogicalExpression or() {
    return new LogicalExpression(LogicalOperator.OR, 0, new ArrayList<>(), new ArrayList<>());
  }

  public static LogicalExpression not() {
    return new LogicalExpression(LogicalOperator.NOT, 0, new ArrayList<>(), new ArrayList<>());
  }

  public void addLogicalExpression(LogicalExpression logicalExpression) {
    // skip empty logic expression
    if (logicalExpression.isEmpty()) {
      return;
    }
    totalComparisonExpressionCount += logicalExpression.getTotalComparisonExpressionCount();
    totalIdComparisonExpressionCount += logicalExpression.getTotalIdComparisonExpressionCount();
    logicalExpressions.add(logicalExpression);
  }

  public void addComparisonExpressions(List<ComparisonExpression> comparisonExpressionList) {
    for (ComparisonExpression comparisonExpression : comparisonExpressionList) {
      if (comparisonExpression.getPath().equals(DocumentConstants.Fields.DOC_ID)) {
        totalIdComparisonExpressionCount++;
      }
      totalComparisonExpressionCount++;
      comparisonExpressions.add(comparisonExpression);
    }
  }

  /**
   * Add childLogicalExpression after it is been flipped with $not operator Will not increment the
   * counter to avoid double count
   *
   * @param logicalExpression logicalExpression
   */
  public void addLogicalExpressionFlipped(LogicalExpression logicalExpression) {
    // skip empty logic expression
    if (logicalExpression.isEmpty()) {
      return;
    }
    logicalExpressions.add(logicalExpression);
  }

  /**
   * Add childComparisonExpressions after they have been flipped with $not operator Will not
   * increment the counter to avoid double count
   *
   * @param comparisonExpressionList comparisonExpressionList
   */
  public void addComparisonExpressionsFlipped(List<ComparisonExpression> comparisonExpressionList) {
    if (comparisonExpressionList.isEmpty()) {
      return;
    }
    comparisonExpressions.addAll(comparisonExpressionList);
  }

  public LogicalOperator getLogicalRelation() {
    return logicalRelation;
  }

  protected void setLogicalRelation(LogicalOperator logicalRelation) {
    this.logicalRelation = logicalRelation;
  }

  public int getTotalComparisonExpressionCount() {
    return totalComparisonExpressionCount;
  }

  public int getTotalIdComparisonExpressionCount() {
    return totalIdComparisonExpressionCount;
  }

  public boolean isEmpty() {
    return logicalExpressions.isEmpty() && comparisonExpressions.isEmpty();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("LogicalExpression{");
    sb.append("logicalRelation='").append(logicalRelation).append("'");
    sb.append(", totalComparisonExpressionCount=").append(totalComparisonExpressionCount);
    sb.append(", logicalExpressions=").append(logicalExpressions);
    sb.append(", comparisonExpressions=").append(comparisonExpressions);
    sb.append("}");
    return sb.toString();
  }
}
