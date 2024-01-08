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

  public enum LogicalOperator {
    AND("$and"),
    OR("$or");
    private String operator;

    LogicalOperator(String operator) {
      this.operator = operator;
    }

    public String getOperator() {
      return operator;
    }
  }

  private final LogicalOperator logicalRelation;
  private int totalComparisonExpressionCount;
  private int totalIdComparisonExpressionCount;

  public List<LogicalExpression> logicalExpressions;
  public List<ComparisonExpression> comparisonExpressions;

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

  public void addLogicalExpression(LogicalExpression logicalExpression) {
    // skip empty logic expression
    if (logicalExpression.isEmpty()) {
      return;
    }
    totalComparisonExpressionCount += logicalExpression.getTotalComparisonExpressionCount();
    totalIdComparisonExpressionCount += logicalExpression.getTotalIdComparisonExpressionCount();
    logicalExpressions.add(logicalExpression);
  }

  public void addComparisonExpression(ComparisonExpression comparisonExpression) {
    // Two counters totalIdComparisonExpressionCount and totalComparisonExpressionCount
    // They are for validating the filters
    // e.g. no more than one ID filter, maximum filter amount
    if (comparisonExpression.getPath().equals(DocumentConstants.Fields.DOC_ID)) {
      totalIdComparisonExpressionCount++;
    }
    totalComparisonExpressionCount++;
    comparisonExpressions.add(comparisonExpression);
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

  public LogicalOperator getLogicalRelation() {
    return logicalRelation;
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
