package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.filters.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import org.bson.types.ObjectId;

/**
 * This object represents conditions based for a json path (node) that need to be tested Spec says
 * you can do this, compare equals to a literal {"username" : "aaron"} This is a shortcut for
 * {"username" : {"$eq" : "aaron"}} In here we expand the shortcut into a canonical long form, so it
 * is all the same.
 */
public class ComparisonExpression {

  private final String path;

  @Valid @NotEmpty private List<FilterOperation<?>> filterOperations;

  private List<DBFilterBase> dbFilters;

  public List<DBFilterBase> getDbFilters() {
    return dbFilters;
  }

  public List<FilterOperation<?>> getFilterOperations() {
    return filterOperations;
  }

  /** This method will be called when not operation is pushed down */
  public void flip() {
    List<FilterOperation<?>> filterOperations = new ArrayList<>(this.filterOperations.size());
    for (FilterOperation<?> filterOperation : this.filterOperations) {
      final FilterOperator flippedOperator = filterOperation.operator().flip();
      JsonLiteral<?> operand =
          getFlippedOperandValue(filterOperation.operator(), filterOperation.operand());
      filterOperations.add(new ValueComparisonOperation<>(flippedOperator, operand));
    }
    this.filterOperations = filterOperations;
  }

  /**
   * This method is used to flip the operand value when not operator is applied, e.g. $exists, $size
   */
  private JsonLiteral<?> getFlippedOperandValue(FilterOperator operator, JsonLiteral<?> operand) {
    if (operator == ElementComparisonOperator.EXISTS) {
      return new JsonLiteral<Boolean>(!((Boolean) operand.value()), operand.type());
    } else if (operator == ArrayComparisonOperator.SIZE) {
      if (((BigDecimal) operand.value()).equals(BigDecimal.ZERO)) {
        // This is the special case, e.g. {"$not":{"ages":{"$size":0}}}
        // A boolean value here means this is to negate size 0
        return new JsonLiteral<Boolean>(true, operand.type());
      }
      return new JsonLiteral<BigDecimal>(((BigDecimal) operand.value()).negate(), operand.type());
    } else {
      return operand;
    }
  }

  public ComparisonExpression(
      @NotBlank(message = "json node path can not be null in filter") String path,
      List<FilterOperation<?>> filterOperations,
      List<DBFilterBase> dbFilters) {
    this.path = path;
    this.filterOperations = filterOperations;
    this.dbFilters = dbFilters;
  }

  /**
   * Shortcut to create equals against a literal, mare condition cannot be added using add().
   *
   * <p>e.g. {"username" : "aaron"}
   *
   * @param path Json node path
   * @param value Value returned by the deserializer
   * @return {@link ComparisonExpression} with equal operator
   */
  public static ComparisonExpression eq(String path, Object value) {
    return new ComparisonExpression(
        path,
        List.of(new ValueComparisonOperation<>(ValueComparisonOperator.EQ, getLiteral(value))),
        null);
  }

  /**
   * Adds a comparison operation
   *
   * <p>e.g. {"username" : "aaron"}
   *
   * @param value Value returned by the deserializer
   * @return {@link ComparisonExpression} with equal operator
   */
  public void add(FilterOperator operator, Object value) {
    filterOperations.add(new ValueComparisonOperation<>(operator, getLiteral(value)));
  }

  public String getPath() {
    return path;
  }

  /**
   * Create Typed JsonLiteral object for the value
   *
   * @param value object came in the request
   * @return {@link JsonLiteral}
   */
  private static JsonLiteral<?> getLiteral(Object value) {
    if (value == null) {
      return new JsonLiteral<>(null, JsonType.NULL);
    }
    if (value instanceof DocumentId) {
      return new JsonLiteral<>((DocumentId) value, JsonType.DOCUMENT_ID);
    }
    if (value instanceof BigDecimal) {
      return new JsonLiteral<>((BigDecimal) value, JsonType.NUMBER);
    }
    if (value instanceof Boolean) {
      return new JsonLiteral<>((Boolean) value, JsonType.BOOLEAN);
    }
    if (value instanceof Date) {
      return new JsonLiteral<>((Date) value, JsonType.DATE);
    }
    if (value instanceof String) {
      return new JsonLiteral<>((String) value, JsonType.STRING);
    }
    if (value instanceof List) {
      return new JsonLiteral<>((List<Object>) value, JsonType.ARRAY);
    }
    if (value instanceof Map) {
      return new JsonLiteral<>((Map<String, Object>) value, JsonType.SUB_DOC);
    }
    if (value instanceof UUID || value instanceof ObjectId) {
      return new JsonLiteral<>(value.toString(), JsonType.STRING);
    }
    throw new JsonApiException(
        ErrorCode.FILTER_UNRESOLVABLE,
        String.format("Unsupported filter value type %s", value.getClass()));
  }

  public List<FilterOperation<?>> match(
      String matchPath, EnumSet<? extends FilterOperator> operator, JsonType type) {
    if ("*".equals(matchPath) || matchPath.equals(path)) {
      return filterOperations.stream()
          .filter(a -> a.match(operator, type))
          .collect(Collectors.toList());
    } else {
      return List.of();
    }
  }

  public void setDBFilters(List<DBFilterBase> dbFilters) {
    this.dbFilters = dbFilters;
  }

  @Override
  public String toString() {
    return "ComparisonExpression{"
        + "path='"
        + path
        + '\''
        + ", filterOperations="
        + filterOperations
        + ", dbFilters="
        + dbFilters
        + '}';
  }
}
