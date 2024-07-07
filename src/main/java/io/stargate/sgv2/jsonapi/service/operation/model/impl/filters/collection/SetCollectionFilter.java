package io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.collection;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.JsonTerm;
import java.util.Objects;

/** DB filter / condition for testing a set value */
public abstract class SetCollectionFilter<T> extends CollectionFilter {
  public enum Operator {
    CONTAINS,
    NOT_CONTAINS;
  }

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
    return columnName.equals(that.columnName)
        && value.equals(that.value)
        && operator == that.operator;
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
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_OPERATION,
            String.format("Unsupported set operation %s on column %s", operator, columnName));
    }
  }
}
