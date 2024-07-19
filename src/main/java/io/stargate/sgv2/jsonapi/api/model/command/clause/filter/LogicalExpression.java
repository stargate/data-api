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
    OR("$or"),
    NOT("$not");
    private String operator;

    LogicalOperator(String operator) {
      this.operator = operator;
    }

    public String getOperator() {
      return operator;
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

  /**
   * This method will flip the operators and operand if logical operator is $not. $not will traverse
   * the expression tree and flip AND->OR, OR->AND, operator->opposite operator
   */
  public void traverseForNot(LogicalExpression parent) {
    // TODO: why this is new array needed ? it is used one to traverse the same list ?
    List<LogicalExpression> tempLogicalExpressions = new ArrayList<>(logicalExpressions);

    for (LogicalExpression logicalExpression : tempLogicalExpressions) {
      logicalExpression.traverseForNot(this);
    }

    Iterator<LogicalExpression> iterator = logicalExpressions.iterator();
    while (iterator.hasNext()) {
      LogicalExpression logicalExpression = iterator.next();
      if (logicalExpression.logicalRelation == LogicalOperator.NOT) {
        // NOT operator, remove it because it is only for flipping other operators
        iterator.remove();

        this.totalComparisonExpressionCount -= logicalExpression.totalComparisonExpressionCount;
        this.totalIdComparisonExpressionCount -= logicalExpression.totalIdComparisonExpressionCount;
      }
    }

    if (logicalRelation == LogicalOperator.NOT) {
      // flip AND->OR, OR->AND, operator->opposite operator
      flip();
      // add to parent logicalExpression after the flip
      addToParent(parent);
    }
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

  private void addToParent(LogicalExpression parent) {
    logicalExpressions.stream().forEach(expression -> parent.addLogicalExpression(expression));
    if (comparisonExpressions.size() > 1) {
      // Multiple conditions in not, after push down will become or
      final LogicalExpression orLogic = LogicalExpression.or();
      comparisonExpressions.stream()
          .forEach(
              comparisonExpression ->
                  orLogic.addComparisonExpressions(List.of(comparisonExpression)));
      parent.addLogicalExpression(orLogic);
    } else {
      if (comparisonExpressions.size() == 1) {
        // Unary not, after push down will become additional condition
        parent.addComparisonExpressions(List.of(comparisonExpressions.get(0)));
      }
    }
    comparisonExpressions.clear();
  }

  private void flip() {
    // TODO: Strongly recommend this class to be immutable, changing the nature of the state like
    // this is dangerous
    if (logicalRelation == LogicalOperator.AND) {
      logicalRelation = LogicalOperator.OR;
    } else if (logicalRelation == LogicalOperator.OR) {
      logicalRelation = LogicalOperator.AND;
    }
    for (LogicalExpression logicalExpression : logicalExpressions) {
      logicalExpression.flip();
    }
    for (ComparisonExpression comparisonExpression : comparisonExpressions) {
      comparisonExpression.flip();
    }
  }
}
