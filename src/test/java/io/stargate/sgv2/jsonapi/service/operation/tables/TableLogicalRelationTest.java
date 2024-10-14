package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests {@link TableWhereCQLClause} apply() to build correct logicalRelation statement */
public class TableLogicalRelationTest {

  private static final TestData TEST_DATA = new TestData();

  private TestDataNames names() {
    return TEST_DATA.names;
  }

  @Test
  public void simpleAnd() {
    var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(eq,eq)");
    var expBuilder = fixture.expressionBuilder;
    var implicitAnd = expBuilder.rootImplicitAnd;
    var dbLogicalExpression =
        implicitAnd.addFilters(
            expBuilder.eq(names().CQL_INT_COLUMN), expBuilder.eq(names().CQL_TEXT_COLUMN));
    fixture
        .expressionBuilder()
        .replaceRootDBLogicalExpression(dbLogicalExpression)
        .applyAndGetOnGoingWhereClause()
        .assertWhereCQL(
            "WHERE (\"%s\"=? AND \"%s\"=?)"
                .formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
        .assertWherePositionalValues(List.of(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
  }

  @Test
  public void simpleOR() {
    var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(OR(eq,eq))");
    var expBuilder = fixture.expressionBuilder;
    var implicitAnd = expBuilder.rootImplicitAnd;
    var dbLogicalExpression =
        implicitAnd.addSubExpression(
            DBLogicalExpression.or(
                expBuilder.eq(names().CQL_INT_COLUMN), expBuilder.eq(names().CQL_TEXT_COLUMN)));
    fixture
        .expressionBuilder()
        .replaceRootDBLogicalExpression(dbLogicalExpression)
        .applyAndGetOnGoingWhereClause()
        .assertWhereCQL(
            "WHERE ((\"%s\"=? OR \"%s\"=?))"
                .formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
        .assertWherePositionalValues(List.of(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
  }

  @Test
  public void tableFilterWithLogicalExpressionOR() {
    var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(eq, OR(eq,eq))");
    var expBuilder = fixture.expressionBuilder;
    var implicitAnd = expBuilder.rootImplicitAnd;
    var dbLogicalExpression =
        implicitAnd
            .addFilters(expBuilder.eq(names().CQL_DATE_COLUMN))
            .addSubExpression(
                DBLogicalExpression.or(
                    expBuilder.eq(names().CQL_INT_COLUMN), expBuilder.eq(names().CQL_TEXT_COLUMN)));
    fixture
        .expressionBuilder()
        .replaceRootDBLogicalExpression(dbLogicalExpression)
        .applyAndGetOnGoingWhereClause()
        .assertWhereCQL(
            "WHERE (\"%s\"=? AND (\"%s\"=? OR \"%s\"=?))"
                .formatted(
                    names().CQL_DATE_COLUMN, names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
        .assertWherePositionalValues(
            List.of(names().CQL_DATE_COLUMN, names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
  }

  @Test
  public void twoLogicalExpressionOR() {
    var fixture =
        TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(OR(eq,eq), OR(eq,eq))");
    var expBuilder = fixture.expressionBuilder;
    var implicitAnd = expBuilder.rootImplicitAnd;
    var dbLogicalExpression =
        implicitAnd
            .addSubExpression(
                DBLogicalExpression.or(
                    expBuilder.eq(names().CQL_INT_COLUMN), expBuilder.eq(names().CQL_TEXT_COLUMN)))
            .addSubExpression(
                DBLogicalExpression.or(
                    expBuilder.eq(names().CQL_DATE_COLUMN),
                    expBuilder.eq(names().CQL_BOOLEAN_COLUMN)));
    fixture
        .expressionBuilder()
        .replaceRootDBLogicalExpression(dbLogicalExpression)
        .applyAndGetOnGoingWhereClause()
        .assertWhereCQL(
            "WHERE ((\"%s\"=? OR \"%s\"=?) AND (\"%s\"=? OR \"%s\"=?))"
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

  @Test
  public void nested_AND_OR() {
    var fixture = TEST_DATA.tableWhereCQLClause().tableWithAllDataTypes("AND(OR(OR(eq,eq)))");
    var expBuilder = fixture.expressionBuilder;
    var implicitAnd = expBuilder.rootImplicitAnd;
    var dbLogicalExpression =
        implicitAnd.addSubExpression(
            DBLogicalExpression.or()
                .addSubExpression(
                    DBLogicalExpression.or(
                        expBuilder.eq(names().CQL_INT_COLUMN),
                        expBuilder.eq(names().CQL_TEXT_COLUMN))));
    fixture
        .expressionBuilder()
        .replaceRootDBLogicalExpression(dbLogicalExpression)
        .applyAndGetOnGoingWhereClause()
        .assertWhereCQL(
            "WHERE (((\"%s\"=? OR \"%s\"=?)))"
                .formatted(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN))
        .assertWherePositionalValues(List.of(names().CQL_INT_COLUMN, names().CQL_TEXT_COLUMN));
  }
}
