package io.stargate.sgv2.jsonapi.service.operation.model.filters.collection;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DATA_CONTAINS;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.BuiltConditionPredicate;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.ConditionLHS;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.JsonTerm;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import java.util.Objects;

/** Filter for the map columns we have in the super shredding table. */
public abstract class MapCollectionFilter<T> extends CollectionFilter {

  // NOTE: we can only do eq until SAI indexes are updated , waiting for >, < etc
  public enum Operator {
    /**
     * This represents eq to be run against map type index columns like array_size, sub_doc_equals
     * and array_equals.
     */
    MAP_EQUALS,
    /**
     * This represents ne to be run against map type index columns like array_size, sub_doc_equals
     * and array_equals.
     */
    MAP_NOT_EQUALS,
    /**
     * This represents eq operation for array element or atomic value operation against
     * array_contains
     */
    EQ,
    /**
     * This represents NE operation for array element or atomic value operation against
     * array_contains
     */
    NE,
    /**
     * This represents greater than to be run against map type index columns for number and date
     * type
     */
    GT,
    /**
     * This represents greater than or equal to be run against map type index columns for number and
     * date type
     */
    GTE,
    /**
     * This represents less than to be run against map type index columns for number and date type
     */
    LT,
    /**
     * This represents lesser than or equal to be run against map type index columns for number and
     * date type
     */
    LTE
  }

  private final String columnName;
  private final String key;
  protected final Operator operator;
  private final T value;

  protected MapCollectionFilter(String columnName, String key, Operator operator, T value) {
    super(key);
    this.columnName = columnName;
    this.key = key;
    this.operator = operator;
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MapCollectionFilter<?> that = (MapCollectionFilter<?>) o;
    return columnName.equals(that.columnName)
        && key.equals(that.key)
        && operator == that.operator
        && value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, key, operator, value);
  }

  @Override
  public BuiltCondition get() {
    switch (operator) {
      case EQ:
        return BuiltCondition.of(
            ConditionLHS.column(DATA_CONTAINS),
            BuiltConditionPredicate.CONTAINS,
            new JsonTerm(getHashValue(new DocValueHasher(), key, value)));
      case NE:
        return BuiltCondition.of(
            ConditionLHS.column(DATA_CONTAINS),
            BuiltConditionPredicate.NOT_CONTAINS,
            new JsonTerm(getHashValue(new DocValueHasher(), key, value)));
      case MAP_EQUALS:
        return BuiltCondition.of(
            ConditionLHS.mapAccess(columnName, key),
            BuiltConditionPredicate.EQ,
            new JsonTerm(key, value));
      case MAP_NOT_EQUALS:
        return BuiltCondition.of(
            ConditionLHS.mapAccess(columnName, key),
            BuiltConditionPredicate.NEQ,
            new JsonTerm(key, value));
      case GT:
        return BuiltCondition.of(
            ConditionLHS.mapAccess(columnName, key),
            BuiltConditionPredicate.GT,
            new JsonTerm(key, value));
      case GTE:
        return BuiltCondition.of(
            ConditionLHS.mapAccess(columnName, key),
            BuiltConditionPredicate.GTE,
            new JsonTerm(key, value));
      case LT:
        return BuiltCondition.of(
            ConditionLHS.mapAccess(columnName, key),
            BuiltConditionPredicate.LT,
            new JsonTerm(key, value));
      case LTE:
        return BuiltCondition.of(
            ConditionLHS.mapAccess(columnName, key),
            BuiltConditionPredicate.LTE,
            new JsonTerm(key, value));
      default:
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_OPERATION,
            String.format("Unsupported map operation %s on column %s", operator, columnName));
    }
  }
}
