package io.stargate.sgv2.jsonapi.service.operation.query;

import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * DBLogicalExpression is what we use in DB level. (LogicalExpression in API level) Similarly to
 * LogicalExpression, this class also has recursive definition by having logical relation context.
 *
 * <p>fields dbLogicalExpressionList and dbFilterList are mutable, because we need to construct the
 * DBLogicalExpression recursively by adding new dbFilters and subDbLogicalExpression.
 */
public class DBLogicalExpression implements Recordable {

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

  private final DBLogicalOperator operator;
  private final List<DBFilterBase> filters = new ArrayList<>();
  private final List<DBLogicalExpression> subExpressions = new ArrayList<>();

  public DBLogicalExpression(DBLogicalOperator operator) {
    this.operator = operator;
  }

  public <T> void visitAllFilters(Class<T> filterClass, Consumer<T> analyzeRule) {

    filters.stream().filter(filterClass::isInstance).map(filterClass::cast).forEach(analyzeRule);

    subExpressions.forEach(
        subExpression -> subExpression.visitAllFilters(filterClass, analyzeRule));
  }

  /**
   * Add a sub dbLogicalExpression as subExpression to current caller dbLogicalExpression. Return
   * the passing sub dbLogicalExpression.
   */
  public DBLogicalExpression addSubExpressionReturnSub(DBLogicalExpression subExpression) {
    subExpressions.add(Objects.requireNonNull(subExpression, "subExpressions cannot be null"));
    return subExpression;
  }

  /**
   * Add a sub dbLogicalExpression as subExpression to current caller dbLogicalExpression. Return
   * the current caller dbLogicalExpression.
   */
  public DBLogicalExpression addSubExpressionReturnCurrent(DBLogicalExpression subExpression) {
    subExpressions.add(Objects.requireNonNull(subExpression, "subExpressions cannot be null"));
    return this;
  }

  /**
   * Add a dbFilter to current caller dbLogicalExpression. This new DBFilter will be added in the
   * dbFilter List, it will be in the relation context of this dbLogicalExpression.
   *
   * @param DBFilterBase filter
   * @return DBFilterBase
   */
  public DBFilterBase addFilter(DBFilterBase filter) {
    filters.add(Objects.requireNonNull(filter, "filter cannot be null"));
    return filter;
  }

  public DBLogicalOperator operator() {
    return operator;
  }

  public boolean isEmpty() {
    return subExpressions.isEmpty() && filters.isEmpty();
  }

  public List<DBFilterBase> filters() {
    return List.copyOf(filters);
  }

  public List<DBLogicalExpression> subExpressions() {
    return List.copyOf(subExpressions);
  }

  /**
   * Count dBFilters amount in DBLogicalExpression. This method will recursively sum up all sub
   * DBLogicalExpression.
   *
   * @return int
   */
  public int totalFilterCount() {
    var childCounts =
        subExpressions().stream().mapToInt(DBLogicalExpression::totalFilterCount).sum();
    return childCounts + filters.size();
  }

  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder
        .append("operator", operator)
        .append("subExpressions", subExpressions)
        .append("filters", filters);
  }
}
