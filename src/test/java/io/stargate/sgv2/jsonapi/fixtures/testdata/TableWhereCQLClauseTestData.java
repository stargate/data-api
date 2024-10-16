package io.stargate.sgv2.jsonapi.fixtures.testdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.override.ExtendedSelect;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableWhereCQLClause;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Notice, the table metadata does not matter in this class,since tests are focusing on
 *
 * <p>1. OR/AND logical relation overrides for driver queryBuilder. 2. Resolve DBLogicalExpression
 * to queryBuilder logical relations
 */
public class TableWhereCQLClauseTestData extends TestDataSuplier {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableWhereCQLClauseTestData.class);

  public TableWhereCQLClauseTestData(TestData testData) {
    super(testData);
  }

  public TableWhereCQLClauseTestData.TableWhereCQLClauseFixture tableWithAllDataTypes(
      String message) {
    var tableMetaData = testData.tableMetadata().tableAllDatatypesIndexed();
    return new TableWhereCQLClauseTestData.TableWhereCQLClauseFixture(
        message, tableMetaData, testData.logicalExpression().implicitAndExpression(tableMetaData));
  }

  public static class TableWhereCQLClauseFixture implements PrettyPrintable {

    private final String message;
    private final TableMetadata tableMetadata;
    private final TableSchemaObject tableSchemaObject;
    public final LogicalExpressionTestData.ExpressionBuilder<TableWhereCQLClauseFixture>
        expressionBuilder;

    private TableWhereCQLClause<Select> tableWhereCQLClause = null;
    private final List<Object> positionalValues = new ArrayList<>();
    private Select onGoingWhereClause = null;
    public Throwable exception = null;

    public LogicalExpressionTestData.ExpressionBuilder<TableWhereCQLClauseFixture>
        expressionBuilder() {
      return expressionBuilder;
    }

    public TableWhereCQLClauseFixture(
        String message, TableMetadata tableMetadata, DBLogicalExpression expressionBuilder) {
      this.message = message;
      this.tableMetadata = tableMetadata;
      this.tableSchemaObject = TableSchemaObject.from(tableMetadata, new ObjectMapper());
      this.expressionBuilder =
          new LogicalExpressionTestData.ExpressionBuilder<>(this, expressionBuilder, tableMetadata);
    }

    public TableWhereCQLClauseFixture applyAndGetOnGoingWhereClause() {
      this.tableWhereCQLClause =
          TableWhereCQLClause.forSelect(tableSchemaObject, expressionBuilder.rootImplicitAnd);
      assertDoesNotThrow(this::callApply, "Error when: %s".formatted(message));
      assertThat(onGoingWhereClause)
          .as("apply result is not null when: %s".formatted(message))
          .isNotNull();
      return this;
    }

    public TableWhereCQLClauseFixture assertWhereCQL(String expectedWhereCQL) {
      LOGGER.warn("Expected where CQL: {}\n", expectedWhereCQL);
      LOGGER.warn("Actual whole CQL: {}\n", onGoingWhereClause.asCql());
      assertThat(onGoingWhereClause.asCql()).contains(expectedWhereCQL);
      return this;
    }

    public TableWhereCQLClauseFixture assertNoWhereClause() {
      LOGGER.warn("Expected NO where clause in CQL statement. \n");
      LOGGER.warn("Actual whole CQL: {}\n", onGoingWhereClause.asCql());
      assertThat(onGoingWhereClause.asCql()).doesNotContain("WHERE");
      return this;
    }

    public TableWhereCQLClauseFixture assertWherePositionalValues(
        List<CqlIdentifier> expectedColumnIdentifiers) {
      List<Object> expectedPositionalValues =
          expectedColumnIdentifiers.stream().map(expressionBuilder::CqlValue).toList();
      LOGGER.warn("Expected positional values: {}\n", expectedPositionalValues);
      LOGGER.warn("Actual positional values:: {}\n", positionalValues);

      // The order of positional values matters here.
      // E.G.
      // Expected values [25, "text-value"], Actual values [25, "text-value"]
      assertThat(expectedPositionalValues).containsExactlyElementsOf(positionalValues);

      return this;
    }

    public TableWhereCQLClauseFixture assertNoPositionalValues() {
      LOGGER.warn("Expected NO positional values.\n");
      LOGGER.warn("Actual positional values:: {}\n", positionalValues);
      assertThat(positionalValues).isEmpty();
      return this;
    }

    private void callApply() {
      LOGGER.warn("Apply WhereCQLClause: {}\n {}", message, toString(true));
      // Select from all columns
      var selectFrom =
          ExtendedSelect.selectFrom(tableMetadata.getKeyspace(), tableMetadata.getName());
      var select = selectFrom.all();
      onGoingWhereClause = tableWhereCQLClause.apply(select, positionalValues);
      LOGGER.warn("Apply WhereCQLClause result(OngoingWhereClause): {}", onGoingWhereClause);
    }

    @Override
    public String toString() {
      return toString(false);
    }

    public String toString(boolean pretty) {
      return toString(new PrettyToStringBuilder(getClass(), pretty)).toString();
    }

    public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
      prettyToStringBuilder
          .append("expression", expressionBuilder.rootImplicitAnd)
          .append("table", tableMetadata.describe(true));
      return prettyToStringBuilder;
    }

    @Override
    public PrettyToStringBuilder appendTo(PrettyToStringBuilder prettyToStringBuilder) {
      var sb = prettyToStringBuilder.beginSubBuilder(getClass());
      return toString(sb).endSubBuilder();
    }
  }
}
