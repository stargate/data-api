package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import jakarta.validation.constraints.NotBlank;
import java.util.*;

/**
 * This object represents conditions based for a json path (node) that need to be tested Spec says
 * you can do this, compare equals to a literal {"username" : "aaron"} This is a shortcut for
 * {"username" : {"$eq" : "aaron"}} In here we expand the shortcut into a canonical long form, so it
 * is all the same.
 */
public class LogicalExpression {

  public static final String OR = "or";
  public static final String AND = "and";

  @NotBlank(message = "logical relation of attribute logicalExpressions")
  String logicalRelation;

  public int totalComparisonExpressionCount;

  public int totalIdComparisonExpressionCount;
  public List<LogicalExpression> logicalExpressions;
  public List<ComparisonExpression> comparisonExpressions;

  private LogicalExpression(
      String logicalRelation,
      int totalComparisonExpressionCount,
      List<LogicalExpression> logicalExpressions,
      List<ComparisonExpression> comparisonExpression) {
    this.logicalRelation = logicalRelation;
    this.totalComparisonExpressionCount = totalComparisonExpressionCount;
    this.logicalExpressions = logicalExpressions;
    this.comparisonExpressions = comparisonExpression;
  }

  public static LogicalExpression and() {
    return new LogicalExpression(AND, 0, new ArrayList<>(), new ArrayList<>());
  }

  public static LogicalExpression or() {
    return new LogicalExpression(OR, 0, new ArrayList<>(), new ArrayList<>());
  }

  public void addLogicalExpression(LogicalExpression logicalExpression) {
    totalComparisonExpressionCount += logicalExpression.totalComparisonExpressionCount;
    totalIdComparisonExpressionCount += logicalExpression.totalIdComparisonExpressionCount;
    if (logicalExpression.logicalExpressions.isEmpty()
        && logicalExpression.comparisonExpressions.isEmpty()) {
      return;
    }
    logicalExpressions.add(logicalExpression);
  }

  public void addComparisonExpression(ComparisonExpression comparisonExpression) {
    if (comparisonExpression.getPath().equals(DocumentConstants.Fields.DOC_ID)) {
      totalIdComparisonExpressionCount++;
    }
    totalComparisonExpressionCount++;
    comparisonExpressions.add(comparisonExpression);
  }

  public String getLogicalRelation() {
    return logicalRelation;
  }

  @Override
  public String toString() {
    return "LogicalExpression{"
        + "logicalRelation='"
        + logicalRelation
        + '\''
        + ", totalComparisonExpressionCount="
        + totalComparisonExpressionCount
        + ", logicalExpressions="
        + logicalExpressions
        + ", comparisonExpressions="
        + comparisonExpressions
        + '}';
  }
}
