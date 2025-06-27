package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListFilterComponent;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import java.math.BigDecimal;
import java.util.*;

/**
 * This object represents conditions based for a json path (node) that need to be tested Spec says
 * you can do this, compare equals to a literal {"username" : "aaron"} This is a shortcut for
 * {"username" : {"$eq" : "aaron"}} In here we expand the shortcut into a canonical long form, so it
 * is all the same.
 */
public class ComparisonExpression implements Invertible {

  /**
   * The json node string representing the filter path. E.G. <code>name</code> for <code>
   * {"name" : "aaron"}</code>
   */
  private final String path;

  /**
   * The nullable enum representing the map/set/list component aims to filter on.
   *
   * <p>For a Collection, mapSetListComponent will be null. For a Table, mapSetListComponent will be
   * null for scalar column path, will be set for map/set/list column path.
   */
  private final MapSetListFilterComponent mapSetListComponent;

  private List<FilterOperation<?>> filterOperations;

  private List<DBFilterBase> dbFilters;

  public ComparisonExpression(
      String path, List<FilterOperation<?>> filterOperations, List<DBFilterBase> dbFilters) {
    this(path, filterOperations, dbFilters, null);
  }

  public ComparisonExpression(
      String path,
      List<FilterOperation<?>> filterOperations,
      List<DBFilterBase> dbFilters,
      MapSetListFilterComponent mapSetListComponent) {
    this.path = path;
    this.mapSetListComponent = mapSetListComponent;
    // the list is mutable via the add() method, make defensive copy when exposing it
    this.filterOperations = filterOperations;
    this.dbFilters = dbFilters;
  }

  /**
   * The Shortcut to create ComparisonExpression with EQ filterOperator. E.G.
   *
   * <ul>
   *   <li><code>{"filter": {"username" : "aaron"}}</code> shortcut form for EQ
   *   <li><code>{"filter": {"username" : {"$eq": "aaron"}}}</code> full form for EQ, using {@link
   *       ComparisonExpression#add(FilterOperator, Object)}
   * </ul>
   *
   * @param path json node path representing the filter path.
   * @param value plain object value from request filter json body.
   * @return {@link ComparisonExpression} with equal operator,
   */
  public static ComparisonExpression eq(String path, Object value) {
    return new ComparisonExpression(
        path, List.of(ValueComparisonOperation.build(ValueComparisonOperator.EQ, value)), null);
  }

  /**
   * Adds a comparison operation using given filterOperator and plain value to the filter
   * operations. E.G.
   *
   * <ul>
   *   <li><code>{"filter": {"username" : "aaron"}}</code> will be EQ operator with value "aaron"
   *   <li><code>{"filter": {"age":{"$lt": 18}}}</code> will be LT operator with value 18
   * </ul>
   */
  public void add(FilterOperator operator, Object value) {
    filterOperations.add(ValueComparisonOperation.build(operator, value));
  }

  /**
   * This method is used to match the path and operator with the filter operations.
   *
   * @param matchPath The path to match.
   * @param operator The operator to match.
   * @param type The type of the value.
   * @param appliesToTableMapSetList if true, the expression must be for a flagged as applying to
   *     table map/set/list column. We need an explicit flag because the operations to filter on
   *     map/set/list columns look the same as for a JSON array in a collection document. See XXXX
   * @return List of FilterOperation that match the criteria.
   */
  public List<FilterOperation<?>> match(
      String matchPath,
      Set<? extends FilterOperator> operator,
      JsonType type,
      boolean appliesToTableMapSetList) {

    // Two sanity checks, Capture should align with the ComparisonExpression to start the match.
    if (appliesToTableMapSetList && this.mapSetListComponent == null) {
      // this expression is not for a table map/set/list column, but the caller wants it to be.
      return List.of();
    }
    if (!appliesToTableMapSetList && this.mapSetListComponent != null) {
      // this expression is for a table map/set/list column, but the caller does not want it to be.
      return List.of();
    }

    // this is comparing JSON paths, so we need a case-sensitive match.
    if ("*".equals(matchPath) || matchPath.equals(path)) {
      return filterOperations.stream()
          .filter(a -> a.match(operator, type, appliesToTableMapSetList))
          .toList();
    } else {
      return List.of();
    }
  }

  /**
   * implements Invertible, method to invert a ComparisonExpression this method will be called when
   * $not operator is pushed down
   */
  @Override
  public Invertible invert() {
    List<FilterOperation<?>> filterOperations = new ArrayList<>(this.filterOperations.size());
    for (FilterOperation<?> filterOperation : this.filterOperations) {
      final FilterOperator invertedOperator = filterOperation.operator().invert();
      JsonLiteral<?> operand =
          getFlippedOperandValue(filterOperation.operator(), filterOperation.operand());
      filterOperations.add(
          new ValueComparisonOperation<>(
              invertedOperator, operand, filterOperation.mapSetListComponent()));
    }
    this.filterOperations = filterOperations;
    return this;
  }

  /**
   * This method is used to flip the operand value when $not logical operator is applied. E.G.
   *
   * <ul>
   *   <li><code>$exists</code> -> flip the boolean operand value
   *   <li><code>$size</code> -> negate the BigDecimal value if not zero, special handling zero
   *       since there is no negating zero
   * </ul>
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

  // Getters, Setters, toString.
  public String getPath() {
    return path;
  }

  public List<FilterOperation<?>> getFilterOperations() {
    return List.copyOf(filterOperations);
  }

  @Override
  public String toString() {
    return "ComparisonExpression{"
        + "path='"
        + path
        + '\''
        + ", mapSetListComponent="
        + mapSetListComponent
        + ", filterOperations="
        + filterOperations
        + ", dbFilters="
        + dbFilters
        + '}';
  }
}
