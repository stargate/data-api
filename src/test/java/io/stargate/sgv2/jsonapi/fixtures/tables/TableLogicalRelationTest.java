package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.query.TableFilter;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for {@link TableWhereCQLClause} to build correct logicalRelation statement */
public class TableLogicalRelationTest {

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
            "WHERE (%s=? AND %s=?)".formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
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
            "WHERE ((%s=? OR %s=?))".formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
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
                or(expBuilder.eq(names().CQL_INT_COLUMN), expBuilder.eq(names().CQL_TEXT_COLUMN)));
    fixture
        .expressionBuilder()
        .replaceRootDBLogicalExpression(dbLogicalExpression)
        .applyAndGetOnGoingWhereClause()
        .assertWhereCQL(
            "WHERE (%s=? AND (%s=? OR %s=?))"
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
        .assertWhereCQL("WHERE (%s=? AND ())".formatted(names().CQL_DATE_COLUMN))
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
            "WHERE ((%s=? OR %s=?) AND (%s=? OR %s=?))"
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
            "WHERE (((%s=? OR %s=?)))".formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
        .assertWherePositionalValues(List.of(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
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
