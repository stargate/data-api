package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import jakarta.validation.constraints.NotBlank;
import java.util.*;
import javax.annotation.Nullable;

/**
 * This object represents conditions based for a json path (node) that need to be tested Spec says
 * you can do this, compare equals to a literal {"username" : "aaron"} This is a shortcut for
 * {"username" : {"$eq" : "aaron"}} In here we expand the shortcut into a canonical long form, so it
 * is all the same.
 */
public record LogicalExpression(
    @NotBlank(message = "logical relation of attribute logicalExpressions") String logicalRelation,
    @Nullable List<LogicalExpression> logicalExpressions,
    @Nullable List<ComparisonExpression> comparisonExpressions) {

  public static LogicalExpression and() {
    return new LogicalExpression("and", new ArrayList<>(), new ArrayList<>());
  }

  public static LogicalExpression or() {
    return new LogicalExpression("or", new ArrayList<>(), new ArrayList<>());
  }

  public void addLogicalExpression(LogicalExpression logicalExpression) {
    if (logicalExpression.logicalExpressions.isEmpty()
        && logicalExpression.comparisonExpressions.isEmpty()) {
      return;
    }
    logicalExpressions.add(logicalExpression);
  }

  public void addComparisonExpression(ComparisonExpression comparisonExpression) {
    comparisonExpressions.add(comparisonExpression);
  }

  public String getLogicalRelation() {
    return logicalRelation;
  }
}
