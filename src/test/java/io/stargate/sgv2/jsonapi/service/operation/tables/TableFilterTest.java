package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.internal.core.type.DefaultListType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;
import com.datastax.oss.driver.internal.core.type.DefaultSetType;
import io.stargate.sgv2.jsonapi.api.model.command.builders.TableFilterClauseBuilderTest;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListFilterComponent;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListTableFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link TableWhereCQLClause} to build correct cql where statement. Note, this suite of
 * unit tests will have table filters and logical expression defined, and then test the expecting
 * cql where statement after applying the filters. For table filter deserialization related tests,
 * go to {@link TableFilterClauseBuilderTest}
 */
class TableFilterTest {

  private static final TestData TEST_DATA = new TestData();

  private TestDataNames names() {
    return TEST_DATA.names;
  }

  /** The filter is empty so only an implicitAnd, so the where clause should be empty as well. */
  @Test
  public void emptyFilter() {
    var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND()");
    var expBuilder = fixture.expressionBuilder;
    var implicitAnd = expBuilder.rootImplicitAnd;

    fixture
        .expressionBuilder()
        .replaceRootDBLogicalExpression(implicitAnd)
        .applyAndGetOnGoingWhereClause()
        .assertNoWhereClause()
        .assertNoPositionalValues();
  }

  @Nested
  class LogicalRelation {
    /**
     * Implicit AND with two eq filters, so two filters should be AND together in the where clause.
     */
    @Test
    public void simpleAnd() {
      var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(eq,eq)");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          addFilters(
              implicitAnd,
              expBuilder.eq(names().CQL_INT_COLUMN),
              expBuilder.eq(names().CQL_TEXT_COLUMN));
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(
              "WHERE (%s = ? AND %s = ?)"
                  .formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
          .assertWherePositionalValues(List.of(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
    }

    /** Implicit AND with an explicit OR includes two eq filters. */
    @Test
    public void simpleOR() {
      var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(OR(eq,eq))");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          implicitAnd.addSubExpressionReturnCurrent(
              or(expBuilder.eq(names().CQL_INT_COLUMN), expBuilder.eq(names().CQL_TEXT_COLUMN)));
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(
              "WHERE ((%s = ? OR %s = ?))"
                  .formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
          .assertWherePositionalValues(List.of(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
    }

    /** Implicit AND with a root-level filter and an explicit OR includes two other eq filters. */
    @Test
    public void tableFilterWithLogicalExpressionOR() {
      var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(eq, OR(eq,eq))");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          addFilters(implicitAnd, expBuilder.eq(names().CQL_DATE_COLUMN))
              .addSubExpressionReturnCurrent(
                  or(
                      expBuilder.eq(names().CQL_INT_COLUMN),
                      expBuilder.eq(names().CQL_TEXT_COLUMN)));
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(
              "WHERE (%s = ? AND (%s = ? OR %s = ?))"
                  .formatted(
                      names().CQL_DATE_COLUMN, names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
          .assertWherePositionalValues(
              List.of(names().CQL_DATE_COLUMN, names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
    }

    /**
     * Implicit AND with a root-level filter and an explicit empty OR. However, the OR is empty, so
     * just empty parentheses are added.
     */
    @Test
    public void tableFilterWithPartialEmptyOR() {
      var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(eq, OR())");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          addFilters(implicitAnd, expBuilder.eq(names().CQL_DATE_COLUMN))
              .addSubExpressionReturnCurrent(or());
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL("WHERE (%s = ? AND ())".formatted(names().CQL_DATE_COLUMN))
          .assertWherePositionalValues(List.of(names().CQL_DATE_COLUMN));
    }

    /** Implicit AND with two explicit ORs, each with two eq filters. */
    @Test
    public void twoLogicalExpressionOR() {
      var fixture =
          TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(OR(eq,eq), OR(eq,eq))");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          implicitAnd
              .addSubExpressionReturnCurrent(
                  or(expBuilder.eq(names().CQL_INT_COLUMN), expBuilder.eq(names().CQL_TEXT_COLUMN)))
              .addSubExpressionReturnCurrent(
                  or(
                      expBuilder.eq(names().CQL_DATE_COLUMN),
                      expBuilder.eq(names().CQL_BOOLEAN_COLUMN)));
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(
              "WHERE ((%s = ? OR %s = ?) AND (%s = ? OR %s = ?))"
                  .formatted(
                      names().CQL_INT_COLUMN,
                      names().CQL_TEXT_COLUMN,
                      names().CQL_DATE_COLUMN,
                      names().CQL_BOOLEAN_COLUMN))
          .assertWherePositionalValues(
              List.of(
                  names().CQL_INT_COLUMN,
                  names().CQL_TEXT_COLUMN,
                  names().CQL_DATE_COLUMN,
                  names().CQL_BOOLEAN_COLUMN));
    }

    /** Implicit AND with two explicit ORs, The second OR is nested inside the first OR. */
    @Test
    public void nested_AND_OR() {
      var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(OR(OR(eq,eq)))");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          implicitAnd.addSubExpressionReturnCurrent(
              or().addSubExpressionReturnCurrent(
                      or(
                          expBuilder.eq(names().CQL_INT_COLUMN),
                          expBuilder.eq(names().CQL_TEXT_COLUMN))));
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(
              "WHERE (((%s = ? OR %s = ?)))"
                  .formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
          .assertWherePositionalValues(List.of(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
    }
  }

  @Nested
  class MapSetListFilter {

    private static Stream<Arguments> listSetFilters() {
      return Stream.of(
          Arguments.of(
              MapSetListTableFilter.Operator.IN, "WHERE ((%s CONTAINS ? OR %s CONTAINS ?))"),
          Arguments.of(
              MapSetListTableFilter.Operator.NIN,
              "WHERE ((%s NOT CONTAINS ? AND %s NOT CONTAINS ?))"),
          Arguments.of(
              MapSetListTableFilter.Operator.ALL, "WHERE ((%s CONTAINS ? AND %s CONTAINS ?))"),
          Arguments.of(
              MapSetListTableFilter.Operator.NOT_ANY,
              "WHERE ((%s NOT CONTAINS ? OR %s NOT CONTAINS ?))"));
    }

    @ParameterizedTest
    @MethodSource("listSetFilters")
    public void listFilter(MapSetListTableFilter.Operator operator, String whereClause) {
      var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("List Filter Test");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          addFilters(
              implicitAnd,
              expBuilder.filterOnMapSetList(
                  names().CQL_LIST_COLUMN, operator, MapSetListFilterComponent.LIST_VALUE));

      var listType =
          (DefaultListType)
              fixture.tableMetadata.getColumn(names().CQL_LIST_COLUMN).get().getType();
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(whereClause.formatted(names().CQL_LIST_COLUMN, names().CQL_LIST_COLUMN))
          .assertWherePositionalValuesByDataType(
              List.of(listType.getElementType(), listType.getElementType()));
    }

    @ParameterizedTest
    @MethodSource("listSetFilters")
    public void setFilter(MapSetListTableFilter.Operator operator, String whereClause) {
      var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("Set Filter Test");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          addFilters(
              implicitAnd,
              expBuilder.filterOnMapSetList(
                  names().CQL_SET_COLUMN, operator, MapSetListFilterComponent.SET_VALUE));

      var setType =
          (DefaultSetType) fixture.tableMetadata.getColumn(names().CQL_SET_COLUMN).get().getType();
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(whereClause.formatted(names().CQL_SET_COLUMN, names().CQL_SET_COLUMN))
          .assertWherePositionalValuesByDataType(
              List.of(setType.getElementType(), setType.getElementType()));
    }

    private static Stream<Arguments> mapKeyOrValueFilters() {
      return Stream.of(
          Arguments.of(
              MapSetListTableFilter.Operator.IN,
              "WHERE ((%s CONTAINS KEY ? OR %s CONTAINS KEY ?))",
              MapSetListFilterComponent.MAP_KEY),
          Arguments.of(
              MapSetListTableFilter.Operator.NIN,
              "WHERE ((%s NOT CONTAINS KEY ? AND %s NOT CONTAINS KEY ?))",
              MapSetListFilterComponent.MAP_KEY),
          Arguments.of(
              MapSetListTableFilter.Operator.ALL,
              "WHERE ((%s CONTAINS KEY ? AND %s CONTAINS KEY ?))",
              MapSetListFilterComponent.MAP_KEY),
          Arguments.of(
              MapSetListTableFilter.Operator.NOT_ANY,
              "WHERE ((%s NOT CONTAINS KEY ? OR %s NOT CONTAINS KEY ?))",
              MapSetListFilterComponent.MAP_KEY),
          Arguments.of(
              MapSetListTableFilter.Operator.IN,
              "WHERE ((%s CONTAINS ? OR %s CONTAINS ?))",
              MapSetListFilterComponent.MAP_VALUE),
          Arguments.of(
              MapSetListTableFilter.Operator.NIN,
              "WHERE ((%s NOT CONTAINS ? AND %s NOT CONTAINS ?))",
              MapSetListFilterComponent.MAP_VALUE),
          Arguments.of(
              MapSetListTableFilter.Operator.ALL,
              "WHERE ((%s CONTAINS ? AND %s CONTAINS ?))",
              MapSetListFilterComponent.MAP_VALUE),
          Arguments.of(
              MapSetListTableFilter.Operator.NOT_ANY,
              "WHERE ((%s NOT CONTAINS ? OR %s NOT CONTAINS ?))",
              MapSetListFilterComponent.MAP_VALUE));
    }

    @ParameterizedTest
    @MethodSource("mapKeyOrValueFilters")
    public void mapKeyOrValueFilter(
        MapSetListTableFilter.Operator operator,
        String whereClause,
        MapSetListFilterComponent keyOrValue) {
      var fixture =
          TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("Map Key/Value Filter Test");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          addFilters(
              implicitAnd,
              expBuilder.filterOnMapSetList(names().CQL_MAP_COLUMN, operator, keyOrValue));

      var map =
          (DefaultMapType) fixture.tableMetadata.getColumn(names().CQL_MAP_COLUMN).get().getType();
      var targetElementType =
          keyOrValue == MapSetListFilterComponent.MAP_KEY ? map.getKeyType() : map.getValueType();
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(whereClause.formatted(names().CQL_MAP_COLUMN, names().CQL_MAP_COLUMN))
          .assertWherePositionalValuesByDataType(List.of(targetElementType, targetElementType));
    }

    private static Stream<Arguments> mapEntryFilters() {
      return Stream.of(
          Arguments.of(
              MapSetListTableFilter.Operator.IN,
              "WHERE ((\"%s\"[?] = ? OR \"%s\"[?] = ?))",
              MapSetListFilterComponent.MAP_KEY),
          Arguments.of(
              MapSetListTableFilter.Operator.NIN,
              "WHERE ((\"%s\"[?] != ? AND \"%s\"[?] != ?))",
              MapSetListFilterComponent.MAP_KEY),
          Arguments.of(
              MapSetListTableFilter.Operator.ALL,
              "WHERE ((\"%s\"[?] = ? AND \"%s\"[?] = ?))",
              MapSetListFilterComponent.MAP_KEY),
          Arguments.of(
              MapSetListTableFilter.Operator.NOT_ANY,
              "WHERE ((\"%s\"[?] != ? OR \"%s\"[?] != ?))",
              MapSetListFilterComponent.MAP_KEY));
    }

    @ParameterizedTest
    @MethodSource("mapEntryFilters")
    public void mapEntryFilters(MapSetListTableFilter.Operator operator, String whereClause) {
      var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("Map Entry Filter Test");
      var expBuilder = fixture.expressionBuilder;
      var implicitAnd = expBuilder.rootImplicitAnd;
      var dbLogicalExpression =
          addFilters(
              implicitAnd,
              expBuilder.filterOnMapSetList(
                  names().CQL_MAP_COLUMN, operator, MapSetListFilterComponent.MAP_ENTRY));

      var map =
          (DefaultMapType) fixture.tableMetadata.getColumn(names().CQL_MAP_COLUMN).get().getType();
      var targetKeyType = map.getKeyType();
      var targetValueType = map.getValueType();
      fixture
          .expressionBuilder()
          .replaceRootDBLogicalExpression(dbLogicalExpression)
          .applyAndGetOnGoingWhereClause()
          .assertWhereCQL(whereClause.formatted(names().CQL_MAP_COLUMN, names().CQL_MAP_COLUMN))
          .assertWherePositionalValuesByDataType(
              List.of(targetKeyType, targetValueType, targetKeyType, targetValueType));
    }
  }

  // ==================================================================================================================
  // Methods below are created to help construct DBLogicalExpression in unit tests
  public DBLogicalExpression and() {
    return new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.AND);
  }

  public DBLogicalExpression and(TableFilter... filters) {
    var and = and();
    Arrays.stream(filters).forEach(and::addFilter);
    return and;
  }

  public DBLogicalExpression or() {
    return new DBLogicalExpression(DBLogicalExpression.DBLogicalOperator.OR);
  }

  public DBLogicalExpression or(TableFilter... filters) {
    var or = or();
    Arrays.stream(filters).forEach(or::addFilter);
    return or;
  }

  public DBLogicalExpression addFilters(
      DBLogicalExpression dbLogicalExpression, TableFilter... filters) {
    Arrays.stream(filters).forEach(dbLogicalExpression::addFilter);
    return dbLogicalExpression;
  }
}
