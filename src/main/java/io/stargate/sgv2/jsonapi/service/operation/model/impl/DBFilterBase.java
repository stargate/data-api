package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/** Base for the DB filters / conditions that we want to use with queries */
public abstract class DBFilterBase implements Supplier<BuiltCondition> {
  /** Filter for the map columns we have in the super shredding table. */
  public abstract static class MapFilterBase<T> extends DBFilterBase {

    // NOTE: we can only do eq until SAI indexes are updated , waiting for >, < etc
    public enum Operator {
      MAP_EQUALS,
      EQ
    }

    private final String columnName;
    private final String key;
    private final DBFilterBase.MapFilterBase.Operator operator;
    private final T value;

    /**
     * Atomic values are added to the array_contains field to support $eq on both atomic value and
     * array element
     */
    private static final String DATA_CONTAINS = "array_contains";

    protected MapFilterBase(
        String columnName, String key, MapFilterBase.Operator operator, T value) {
      this.columnName = columnName;
      this.key = key;
      this.operator = operator;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MapFilterBase<?> that = (MapFilterBase<?>) o;
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
              DATA_CONTAINS,
              Predicate.CONTAINS,
              getGrpcValue(getHashValue(new DocValueHasher(), key, value)));
        case MAP_EQUALS:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.of(key)),
              Predicate.EQ,
              getGrpcValue(value));
        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported map operation %s on column %s", operator, columnName));
      }
    }
  }

  /** Filters db documents based on a text field value */
  public static class TextFilter extends MapFilterBase<String> {
    public TextFilter(String path, Operator operator, String value) {
      super("query_text_values", path, operator, value);
    }
  }

  /** Filters db documents based on a boolean field value */
  public static class BoolFilter extends MapFilterBase<Boolean> {
    public BoolFilter(String path, Operator operator, Boolean value) {
      super("query_bool_values", path, operator, value);
    }
  }

  /** Filters db documents based on a numeric field value */
  public static class NumberFilter extends MapFilterBase<BigDecimal> {
    public NumberFilter(String path, Operator operator, BigDecimal value) {
      super("query_dbl_values", path, operator, value);
    }
  }

  /** Filters db documents based on a document id field value */
  public static class IDFilter extends DBFilterBase {
    public enum Operator {
      EQ;
    }

    protected final IDFilter.Operator operator;
    protected final DocumentId value;

    public IDFilter(IDFilter.Operator operator, DocumentId value) {
      this.operator = operator;
      this.value = value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IDFilter idFilter = (IDFilter) o;
      return operator == idFilter.operator && value.equals(idFilter.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operator, value);
    }

    @Override
    public BuiltCondition get() {
      switch (operator) {
        case EQ:
          return BuiltCondition.of(
              BuiltCondition.LHS.column("key"), Predicate.EQ, getDocumentIdValue(value));
        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported id column operation %s", operator));
      }
    }
  }
  /**
   * DB filter / condition for testing a set value Note: we can only do CONTAINS until SAI indexes
   * are updated
   */
  public abstract static class SetFilterBase<T> extends DBFilterBase {
    public enum Operator {
      CONTAINS;
    }

    protected final String columnName;
    protected final T value;
    protected final SetFilterBase.Operator operator;

    protected SetFilterBase(String columnName, T value, SetFilterBase.Operator operator) {
      this.columnName = columnName;
      this.value = value;
      this.operator = operator;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      SetFilterBase<?> that = (SetFilterBase<?>) o;
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
          return BuiltCondition.of(columnName, Predicate.CONTAINS, getGrpcValue(value));
        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported set operation %s on column %s", operator, columnName));
      }
    }
  }

  /**
   * Filter for document where a field == null
   *
   * <p>NOTE: cannot do != null until we get NOT CONTAINS in the DB for set
   */
  public static class IsNullFilter extends SetFilterBase<String> {
    public IsNullFilter(String path) {
      super("query_null_values", path, Operator.CONTAINS);
    }
  }

  /**
   * Filter for document where a field exists
   *
   * <p>NOTE: cannot do != null until we get NOT CONTAINS in the DB for set
   */
  public static class ExistsFilter extends SetFilterBase<String> {
    public ExistsFilter(String path, boolean existFlag) {
      super("exist_keys", path, Operator.CONTAINS);
    }
  }

  /** Filter for document where all values exists for an array */
  public static class AllFilter extends SetFilterBase<String> {
    public AllFilter(DocValueHasher hasher, String path, Object arrayValue) {
      super("array_contains", getHashValue(hasher, path, arrayValue), Operator.CONTAINS);
    }
  }

  /** Filter for document where array has specified number of elements */
  public static class SizeFilter extends MapFilterBase<Integer> {
    public SizeFilter(String path, Integer size) {
      super("array_size", path, Operator.MAP_EQUALS, size);
    }
  }

  /** Filter for document where array matches (data in same order) as the array in request */
  public static class ArrayEqualsFilter extends MapFilterBase<String> {
    public ArrayEqualsFilter(DocValueHasher hasher, String path, List<Object> arrayData) {
      super("array_equals", path, Operator.MAP_EQUALS, getHash(hasher, arrayData));
    }
  }

  /**
   * Filter for document where field is subdocument and matches (same subfield in same order) the
   * filter sub document
   */
  public static class SubDocEqualsFilter extends MapFilterBase<String> {
    public SubDocEqualsFilter(DocValueHasher hasher, String path, Map<String, Object> subDocData) {
      super("sub_doc_equals", path, Operator.MAP_EQUALS, getHash(hasher, subDocData));
    }
  }

  private static QueryOuterClass.Value getGrpcValue(Object value) {
    if (value instanceof String) {
      return Values.of((String) value);
    } else if (value instanceof BigDecimal) {
      return Values.of((BigDecimal) value);
    } else if (value instanceof Byte) {
      return Values.of((Byte) value);
    } else if (value instanceof Integer) {
      return Values.of((Integer) value);
    }
    return Values.of((String) null);
  }

  /**
   * @param hasher
   * @param path Path value is prefixed to the hash value of arrays.
   * @param arrayValue
   * @return
   */
  private static String getHashValue(DocValueHasher hasher, String path, Object arrayValue) {
    return path + " " + getHash(hasher, arrayValue);
  }

  private static String getHash(DocValueHasher hasher, Object arrayValue) {
    return hasher.getHash(arrayValue).hash();
  }

  private static QueryOuterClass.Value getDocumentIdValue(DocumentId value) {
    return Values.of(CustomValueSerializers.getDocumentIdValue(value));
  }
}
