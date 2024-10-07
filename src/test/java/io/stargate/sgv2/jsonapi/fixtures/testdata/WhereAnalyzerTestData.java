package io.stargate.sgv2.jsonapi.fixtures.testdata;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtColumnMetadata;
import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmtCqlIdentifier;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.COLUMN_METADATA_COMPARATOR;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.CQL_IDENTIFIER_COMPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.tables.TableWhereCQLClause;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer;
import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WhereAnalyzerTestData extends TestDataSuplier {

  private static final Logger LOGGER = LoggerFactory.getLogger(WhereAnalyzerTestData.class);

  public WhereAnalyzerTestData(TestData testData) {
    super(testData);
  }

  public WhereAnalyzerFixture table2PK3Clustering1Index(String message) {
    var tableMetaData = testData.tableMetadata().table2PK3Clustering1Index();
    return new WhereAnalyzerFixture(
        message, tableMetaData, testData.logicalExpression().andExpression(tableMetaData));
  }

  public WhereAnalyzerFixture tableKeyAndTwoDuration(String message) {
    var tableMetaData = testData.tableMetadata().keyAndTwoDuration();
    return new WhereAnalyzerFixture(
        message, tableMetaData, testData.logicalExpression().andExpression(tableMetaData));
  }

  public static class WhereAnalyzerFixture implements PrettyPrintable {

    private final String message;
    private final TableMetadata tableMetadata;
    private final TableSchemaObject tableSchemaObject;
    private final WhereCQLClauseAnalyzer analyzer;
    private final LogicalExpressionTestData.ExpressionBuilder<WhereAnalyzerFixture> expression;

    private WhereCQLClauseAnalyzer.WhereClauseAnalysis analysisResult = null;
    public Throwable exception = null;

    public WhereAnalyzerFixture(
        String message, TableMetadata tableMetadata, DBLogicalExpression expression) {

      this.message = message;
      this.tableMetadata = tableMetadata;
      this.analyzer =
          new WhereCQLClauseAnalyzer(
              TableSchemaObject.getTableSettings(tableMetadata, new ObjectMapper()));
      this.tableSchemaObject =
          TableSchemaObject.getTableSettings(tableMetadata, new ObjectMapper());
      this.expression =
          new LogicalExpressionTestData.ExpressionBuilder<>(this, expression, tableMetadata);
    }

    public LogicalExpressionTestData.ExpressionBuilder<WhereAnalyzerFixture> expression() {
      return expression;
    }

    public <T extends Throwable> WhereAnalyzerFixture analyzeThrows(Class<T> exceptionClass) {

      this.exception =
          assertThrowsExactly(
              exceptionClass,
              this::callAnalyze,
              "Expected exception %s when: %s".formatted(exceptionClass, message));

      LOGGER.warn("Analysis Error: {}\n {}", message, this.exception.toString());
      return this;
    }

    public WhereAnalyzerFixture analyze() {
      assertDoesNotThrow(this::callAnalyze, "No error when: %s".formatted(message));
      assertThat(analysisResult)
          .as("analysisResult not null when: %s".formatted(message))
          .isNotNull();

      return this;
    }

    public void callAnalyze() {
      LOGGER.warn("Analyzing: {}\n {}", message, toString(true));
      // store the result in this fixture for later
      analysisResult =
          analyzer.analyse(TableWhereCQLClause.forSelect(tableSchemaObject, expression.expression));
      LOGGER.warn("Analysis result: {}", analysisResult);
    }

    public WhereAnalyzerFixture assertFilterExceptionCode(FilterException.Code code) {
      assertThat(exception)
          .as("FilterException with code %s when: %s".formatted(code, message))
          .isInstanceOf(FilterException.class)
          .satisfies(e -> assertThat(((FilterException) e).code).isEqualTo(code.name()));
      return this;
    }

    public WhereAnalyzerFixture assertExceptionOnUnknownColumns(CqlIdentifier... columns) {
      var identifiers = Arrays.stream(columns).sorted(CQL_IDENTIFIER_COMPARATOR).toList();
      var warning =
          "The filter included the following unknown columns: %s."
              .formatted(errFmtCqlIdentifier(identifiers));
      return assertExceptionContains(warning);
    }

    public WhereAnalyzerFixture assertExceptionOnDurationColumns(CqlIdentifier... columns) {
      var identifiers = Arrays.stream(columns).sorted(CQL_IDENTIFIER_COMPARATOR).toList();
      var warning =
          "The request used a comparison operation on duration columns: %s."
              .formatted(errFmtCqlIdentifier(identifiers));
      return assertExceptionContains(warning);
    }

    public WhereAnalyzerFixture assertExceptionContains(String contains) {

      assertThat(exception)
          .as("Exception message contains expected when: %s".formatted(message))
          .isNotNull()
          .hasMessageContaining(contains);
      return this;
    }

    public WhereAnalyzerFixture assertNoFilteringNoWarnings() {
      assertAllowFilteringDisabled();
      assertNoWarnings();
      return this;
    }

    public WhereAnalyzerFixture assertAllowFilteringEnabled() {
      assertThat(analysisResult.requiresAllowFiltering())
          .as("ALLOW FILTERING enabled when: %s".formatted(message))
          .isTrue();
      return this;
    }

    public WhereAnalyzerFixture assertAllowFilteringDisabled() {
      assertThat(analysisResult.requiresAllowFiltering())
          .as("ALLOW FILTERING disabled when: %s".formatted(message))
          .isFalse();
      return this;
    }

    public WhereAnalyzerFixture assertOneWarning(WarningException.Code warningCode) {

      assertThat(analysisResult.warningExceptions())
          .as("One warning when: %s".formatted(message))
          .hasSize(1)
          .allMatch(exception -> exception.code.equals(warningCode.name()));
      return this;
    }

    public WhereAnalyzerFixture assertWarnOnNotEqColumns(CqlIdentifier... columns) {

      var identifiers = Arrays.stream(columns).sorted(CQL_IDENTIFIER_COMPARATOR).toList();
      var warning =
          "The request applied $ne to the columns: %s.".formatted(errFmtCqlIdentifier(identifiers));
      return assertWarningContains(warning);
    }

    public WhereAnalyzerFixture assertWarnOnUnindexedColumns(CqlIdentifier... columns) {

      var identifiers = Arrays.stream(columns).sorted(CQL_IDENTIFIER_COMPARATOR).toList();
      var warning =
          "The request filtered on the un-indexed columns: %s."
              .formatted(errFmtCqlIdentifier(identifiers));
      return assertWarningContains(warning);
    }

    public WhereAnalyzerFixture assertWarnOnMissingPartitionKeys(CqlIdentifier... columns) {

      var metadata =
          Arrays.stream(columns)
              .map(column -> tableMetadata.getColumn(column).orElseThrow())
              .sorted(COLUMN_METADATA_COMPARATOR)
              .toList();
      var warning = "- Missing Partition Keys: %s.".formatted(errFmtColumnMetadata(metadata));
      return assertWarningContains(warning);
    }

    public WhereAnalyzerFixture assertWarnOnOutOfOrder(CqlIdentifier... columns) {

      var metadata =
          Arrays.stream(columns)
              .map(column -> tableMetadata.getColumn(column).orElseThrow())
              .sorted(COLUMN_METADATA_COMPARATOR)
              .toList();
      var warning =
          "- Out of order Partition Sort Keys: %s.".formatted(errFmtColumnMetadata(metadata));
      return assertWarningContains(warning);
    }

    public WhereAnalyzerFixture assertWarningContains(String contains) {

      assertThat(analysisResult.warningExceptions())
          .as("Warning message contains expected when: %s".formatted(message))
          .hasSize(1)
          .first()
          .satisfies(
              warningException -> {
                assertThat(warningException.getMessage()).contains(contains);
              });
      return this;
    }

    public WhereAnalyzerFixture assertNoWarnings() {
      assertThat(analysisResult.warningExceptions())
          .as("No warnings when: %s".formatted(message))
          .isEmpty();
      return this;
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
          .append("expression", expression.expression)
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
