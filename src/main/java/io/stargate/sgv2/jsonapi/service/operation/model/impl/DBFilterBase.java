package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Base for the DB filters / conditions that we want to use with queries */
public abstract class DBFilterBase implements Supplier<BuiltCondition> {
  /** Filter condition element path. */
  private final String path;

  protected DBFilterBase(String path) {
    this.path = path;
  }

  /**
   * Get JsonNode for the representing filter condition value.
   *
   * @param nodeFactory
   * @return
   */
  abstract JsonNode asJson(JsonNodeFactory nodeFactory);

  /**
   * Returns filter condition element path.
   *
   * @return
   */
  protected String getPath() {
    return path;
  }

  /**
   * Returns `true` if the filter condition should be added to upsert row
   *
   * @return
   */
  abstract boolean canAddField();

  /** Filter for the map columns we have in the super shredding table. */
  public abstract static class MapFilterBase<T> extends DBFilterBase {

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
       * This represents greater than or equal to be run against map type index columns for number
       * and date type
       */
      GTE,
      /**
       * This represents less than to be run against map type index columns for number and date type
       */
      LT,
      /**
       * This represents lesser than or equal to be run against map type index columns for number
       * and date type
       */
      LTE
    }

    private final String columnName;
    private final String key;
    protected final DBFilterBase.MapFilterBase.Operator operator;
    private final T value;

    protected MapFilterBase(
        String columnName, String key, MapFilterBase.Operator operator, T value) {
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
              new JsonTerm(getHashValue(new DocValueHasher(), key, value)));
        case NE:
          return BuiltCondition.of(
              DATA_CONTAINS,
              Predicate.NOT_CONTAINS,
              new JsonTerm(getHashValue(new DocValueHasher(), key, value)));
        case MAP_EQUALS:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.NULL),
              Predicate.EQ,
              new JsonTerm(key, value));
        case MAP_NOT_EQUALS:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.NULL),
              Predicate.NEQ,
              new JsonTerm(key, value));
        case GT:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.NULL),
              Predicate.GT,
              new JsonTerm(key, value));
        case GTE:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.NULL),
              Predicate.GTE,
              new JsonTerm(key, value));
        case LT:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.NULL),
              Predicate.LT,
              new JsonTerm(key, value));
        case LTE:
          return BuiltCondition.of(
              BuiltCondition.LHS.mapAccess(columnName, Values.NULL),
              Predicate.LTE,
              new JsonTerm(key, value));
        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported map operation %s on column %s", operator, columnName));
      }
    }
  }

  /** Filters db documents based on a text field value */
  public static class TextFilter extends MapFilterBase<String> {
    private final String strValue;

    public TextFilter(String path, Operator operator, String value) {
      super("query_text_values", path, operator, value);
      this.strValue = value;
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return nodeFactory.textNode(strValue);
    }

    @Override
    boolean canAddField() {
      return Operator.EQ.equals(operator);
    }
  }

  /** Filters db documents based on a boolean field value */
  public static class BoolFilter extends MapFilterBase<Boolean> {
    private final boolean boolValue;

    public BoolFilter(String path, Operator operator, Boolean value) {
      super("query_bool_values", path, operator, value);
      this.boolValue = value;
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return nodeFactory.booleanNode(boolValue);
    }

    @Override
    boolean canAddField() {
      return Operator.EQ.equals(operator);
    }
  }

  /** Filters db documents based on a numeric field value */
  public static class NumberFilter extends MapFilterBase<BigDecimal> {
    private final BigDecimal numberValue;

    public NumberFilter(String path, Operator operator, BigDecimal value) {
      super("query_dbl_values", path, operator, value);
      this.numberValue = value;
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return nodeFactory.numberNode(numberValue);
    }

    @Override
    boolean canAddField() {
      return Operator.EQ.equals(operator);
    }
  }

  /** Filters db documents based on a date field value */
  public static class DateFilter extends MapFilterBase<Instant> {
    private final Date dateValue;

    public DateFilter(String path, Operator operator, Date value) {
      super("query_timestamp_values", path, operator, Instant.ofEpochMilli(value.getTime()));
      this.dateValue = value;
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return nodeFactory.numberNode(dateValue.getTime());
    }

    @Override
    boolean canAddField() {
      return Operator.EQ.equals(operator);
    }
  }

  /** Filters db documents based on a document id field value */
  public static class IDFilter extends DBFilterBase {
    public enum Operator {
      EQ,
      NE,
      IN
    }

    protected final IDFilter.Operator operator;
    protected final List<DocumentId> values;

    public IDFilter(IDFilter.Operator operator, DocumentId value) {
      this(operator, List.of(value));
    }

    public IDFilter(IDFilter.Operator operator, List<DocumentId> values) {
      super(DOC_ID);
      this.operator = operator;
      this.values = values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IDFilter idFilter = (IDFilter) o;
      return operator == idFilter.operator && Objects.equals(values, idFilter.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(operator, values);
    }

    @Override
    public BuiltCondition get() {
      // For Id filter we always use getALL() method
      return null;
    }

    public List<BuiltCondition> getAll() {
      switch (operator) {
        case EQ:
          return List.of(
              BuiltCondition.of(
                  BuiltCondition.LHS.column("key"),
                  Predicate.EQ,
                  new JsonTerm(CQLBindValues.getDocumentIdValue(values.get(0)))));
        case NE:
          final DocumentId documentId = (DocumentId) values.get(0);
          if (documentId.value() instanceof BigDecimal numberId) {
            return List.of(
                BuiltCondition.of(
                    BuiltCondition.LHS.mapAccess("query_dbl_values", Values.NULL),
                    Predicate.NEQ,
                    new JsonTerm(DOC_ID, numberId)));
          } else if (documentId.value() instanceof String strId) {
            return List.of(
                BuiltCondition.of(
                    BuiltCondition.LHS.mapAccess("query_text_values", Values.NULL),
                    Predicate.NEQ,
                    new JsonTerm(DOC_ID, strId)));
          } else {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
                String.format("Unsupported $ne operand value : %s", documentId.value()));
          }
        case IN:
          if (values.isEmpty()) return List.of();
          return values.stream()
              .map(
                  v ->
                      BuiltCondition.of(
                          BuiltCondition.LHS.column("key"),
                          Predicate.EQ,
                          new JsonTerm(CQLBindValues.getDocumentIdValue(v))))
              .collect(Collectors.toList());
        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported id column operation %s", operator));
      }
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return DBFilterBase.getJsonNode(nodeFactory, values.get(0));
    }

    @Override
    boolean canAddField() {
      if (operator.equals(Operator.EQ)) {
        return true;
      } else {
        return false;
      }
    }
  }

  /** non_id($in, $nin), _id($nin) */
  public static class InFilter extends DBFilterBase {
    private final List<Object> arrayValue;
    protected final InFilter.Operator operator;

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return DBFilterBase.getJsonNode(nodeFactory, arrayValue);
    }

    @Override
    boolean canAddField() {
      return false;
    }

    public enum Operator {
      IN,
      NIN,
    }

    public InFilter(InFilter.Operator operator, String path, List<Object> arrayValue) {
      super(path);
      this.arrayValue = arrayValue;
      this.operator = operator;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      InFilter inFilter = (InFilter) o;
      return operator == inFilter.operator && Objects.equals(arrayValue, inFilter.arrayValue);
    }

    @Override
    public int hashCode() {
      return Objects.hash(arrayValue, operator);
    }

    @Override
    public BuiltCondition get() {
      throw new UnsupportedOperationException("For IN filter we always use getALL() method");
    }

    public List<BuiltCondition> getAll() {
      List<Object> values = arrayValue;
      switch (operator) {
        case IN:
          if (values.isEmpty()) return List.of();
          return values.stream()
              .map(
                  v ->
                      BuiltCondition.of(
                          DATA_CONTAINS,
                          Predicate.CONTAINS,
                          new JsonTerm(getHashValue(new DocValueHasher(), getPath(), v))))
              .collect(Collectors.toList());
        case NIN:
          if (values.isEmpty()) return List.of();
          if (!this.getPath().equals(DOC_ID)) {
            return values.stream()
                .map(
                    v ->
                        BuiltCondition.of(
                            DATA_CONTAINS,
                            Predicate.NOT_CONTAINS,
                            new JsonTerm(getHashValue(new DocValueHasher(), getPath(), v))))
                .collect(Collectors.toList());
          } else {
            // can not use stream here, since lambda parameter casting is not allowed
            List<BuiltCondition> conditions = new ArrayList<>();
            for (Object value : values) {
              if (value instanceof DocumentId) {
                Object docIdValue = ((DocumentId) value).value();
                if (docIdValue instanceof BigDecimal numberId) {
                  BuiltCondition condition =
                      BuiltCondition.of(
                          BuiltCondition.LHS.mapAccess("query_dbl_values", Values.NULL),
                          Predicate.NEQ,
                          new JsonTerm(DOC_ID, numberId));
                  conditions.add(condition);
                } else if (docIdValue instanceof String strId) {
                  BuiltCondition condition =
                      BuiltCondition.of(
                          BuiltCondition.LHS.mapAccess("query_text_values", Values.NULL),
                          Predicate.NEQ,
                          new JsonTerm(DOC_ID, strId));
                  conditions.add(condition);
                } else {
                  throw new JsonApiException(
                      ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
                      String.format("Unsupported $nin operand value: %s", docIdValue));
                }
              }
            }
            return conditions;
          }
        default:
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_FILTER_OPERATION,
              String.format("Unsupported %s column operation %s", getPath(), operator));
      }
    }
  }

  /** DB filter / condition for testing a set value */
  public abstract static class SetFilterBase<T> extends DBFilterBase {
    public enum Operator {
      CONTAINS,
      NOT_CONTAINS;
    }

    protected final String columnName;
    protected final T value;
    protected final SetFilterBase.Operator operator;

    protected SetFilterBase(
        String columnName, String filterPath, T value, SetFilterBase.Operator operator) {
      super(filterPath);
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
          return BuiltCondition.of(columnName, Predicate.CONTAINS, new JsonTerm(value));
        case NOT_CONTAINS:
          return BuiltCondition.of(columnName, Predicate.NOT_CONTAINS, new JsonTerm(value));
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
    public IsNullFilter(String path, SetFilterBase.Operator operator) {
      super("query_null_values", path, path, operator);
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return DBFilterBase.getJsonNode(nodeFactory, null);
    }

    @Override
    boolean canAddField() {
      return true;
    }
  }

  /**
   * Filter for document where a field exists or not
   *
   * <p>NOTE: cannot do != null until we get NOT CONTAINS in the DB for set ?????TODO
   */
  public static class ExistsFilter extends SetFilterBase<String> {
    public ExistsFilter(String path, boolean existFlag) {
      super("exist_keys", path, path, existFlag ? Operator.CONTAINS : Operator.NOT_CONTAINS);
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return null;
    }

    @Override
    boolean canAddField() {
      return false;
    }
  }

  /** Filter for document where all values exists for an array */
  public static class AllFilter extends SetFilterBase<String> {
    private final Object arrayValue;

    public AllFilter(DocValueHasher hasher, String path, Object arrayValue) {
      super("array_contains", path, getHashValue(hasher, path, arrayValue), Operator.CONTAINS);
      this.arrayValue = arrayValue;
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return DBFilterBase.getJsonNode(nodeFactory, arrayValue);
    }

    @Override
    boolean canAddField() {
      return false;
    }
  }

  /** Filter for document where array has specified number of elements */
  public static class SizeFilter extends MapFilterBase<Integer> {
    public SizeFilter(String path, Integer size) {
      super("array_size", path, Operator.MAP_EQUALS, size);
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return null;
    }

    @Override
    boolean canAddField() {
      return false;
    }
  }
  /** Filter for document where array matches (data in same order) as the array in request */
  public static class ArrayEqualsFilter extends MapFilterBase<String> {
    private final List<Object> arrayValue;

    public ArrayEqualsFilter(
        DocValueHasher hasher,
        String path,
        List<Object> arrayData,
        MapFilterBase.Operator operator) {
      super("query_text_values", path, operator, getHash(hasher, arrayData));
      this.arrayValue = arrayData;
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return DBFilterBase.getJsonNode(nodeFactory, arrayValue);
    }

    @Override
    boolean canAddField() {
      return true;
    }
  }

  /**
   * Filter for document where field is subdocument and matches (same subfield in same order) the
   * filter sub document
   */
  public static class SubDocEqualsFilter extends MapFilterBase<String> {
    private final Map<String, Object> subDocValue;

    public SubDocEqualsFilter(
        DocValueHasher hasher,
        String path,
        Map<String, Object> subDocData,
        MapFilterBase.Operator operator) {
      super("query_text_values", path, operator, getHash(hasher, subDocData));
      this.subDocValue = subDocData;
    }

    @Override
    JsonNode asJson(JsonNodeFactory nodeFactory) {
      return DBFilterBase.getJsonNode(nodeFactory, subDocValue);
    }

    @Override
    boolean canAddField() {
      return true;
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
    } else if (value instanceof Date) {
      return Values.of(((Date) value).getTime());
    }
    return Values.of((String) null);
  }

  /**
   * Return JsonNode for a filter conditions value, used to set in new document created for upsert.
   *
   * @param nodeFactory
   * @param value
   * @return
   */
  private static JsonNode getJsonNode(JsonNodeFactory nodeFactory, Object value) {
    if (value == null) return nodeFactory.nullNode();
    if (value instanceof DocumentId) {
      return ((DocumentId) value).asJson(nodeFactory);
    } else if (value instanceof String) {
      return nodeFactory.textNode((String) value);
    } else if (value instanceof BigDecimal) {
      return nodeFactory.numberNode((BigDecimal) value);
    } else if (value instanceof Boolean) {
      return nodeFactory.booleanNode((Boolean) value);
    } else if (value instanceof Date) {
      return JsonUtil.createEJSonDate(nodeFactory, (Date) value);
    } else if (value instanceof List) {
      List<Object> listValues = (List<Object>) value;
      final ArrayNode arrayNode = nodeFactory.arrayNode(listValues.size());
      listValues.forEach(listValue -> arrayNode.add(getJsonNode(nodeFactory, listValue)));
      return arrayNode;
    } else if (value instanceof Map) {
      Map<String, Object> mapValues = (Map<String, Object>) value;
      final ObjectNode objectNode = nodeFactory.objectNode();
      mapValues
          .entrySet()
          .forEach(kv -> objectNode.put(kv.getKey(), getJsonNode(nodeFactory, kv.getValue())));
      return objectNode;
    }
    return nodeFactory.nullNode();
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
