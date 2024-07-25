package io.stargate.sgv2.jsonapi.service.operation.filters.collection;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.builder.JsonTerm;
import java.util.Objects;

/** DB filter / condition for testing a set value */
public abstract class SetCollectionFilter<T> extends CollectionFilter {
  public enum Operator {
    CONTAINS,
    NOT_CONTAINS;
  }

  /** Set-valued database column this filter operates on. */
  protected final String columnName;

  protected final T value;
  protected final Operator operator;

  protected SetCollectionFilter(String columnName, String filterPath, T value, Operator operator) {
    super(filterPath);
    this.columnName = columnName;
    this.value = value;
    this.operator = operator;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SetCollectionFilter<?> that = (SetCollectionFilter<?>) o;
    return operator == that.operator
        && columnName.equals(that.columnName)
        && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, value, operator);
  }

  @Override
  public BuiltCondition get() {
    switch (operator) {
      case CONTAINS:
        return BuiltCondition.of(
            ConditionLHS.column(columnName), BuiltConditionPredicate.CONTAINS, new JsonTerm(value));
      case NOT_CONTAINS:
        return BuiltCondition.of(
            ConditionLHS.column(columnName),
            BuiltConditionPredicate.NOT_CONTAINS,
            new JsonTerm(value));
      default:
        throw ErrorCode.UNSUPPORTED_FILTER_OPERATION.toApiException(
            "Set operation '%s' on column '%s'", operator, columnName);
    }
  }
}
