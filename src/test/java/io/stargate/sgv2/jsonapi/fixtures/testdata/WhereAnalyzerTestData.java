package io.stargate.sgv2.jsonapi.fixtures.testdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.operation.tables.WhereCQLClauseAnalyzer;
import java.util.List;
import java.util.stream.Collectors;

public class WhereAnalyzerTestData extends TestDataSuplier {

  public WhereAnalyzerTestData(TestData testData) {
    super(testData);
  }

  public WhereAnalyzerFixture eqAllPrimaryKeys() {
    var tableMetaData = testData.tableMetadata().TableWith2PartitionKeys3ClusteringKeys();
    return newFixture(
        tableMetaData,
        testData.logicalExpression().eqAllPartitionKeysAndClusteringKeys(tableMetaData));
  }

  public WhereAnalyzerFixture eqAllPartitionKeys() {
    var tableMetaData = testData.tableMetadata().TableWith2PartitionKeys3ClusteringKeys();
    return newFixture(
        tableMetaData, testData.logicalExpression().eqAllPartitionKeys(tableMetaData));
  }

  public WhereAnalyzerFixture eqAllPartitionKeysSkipOneClusteringKey() {
    var tableMetaData = testData.tableMetadata().TableWith2PartitionKeys3ClusteringKeys();
    return newFixture(
        tableMetaData,
        testData.logicalExpression().eqAllPartitionKeysAndSkippingOneClusteringKey(tableMetaData));
  }

  // This is the filter against a column(not PK, without SAI) that exists on table.
  public WhereAnalyzerFixture eqRegularColumnFilterWithoutIndex() {
    var tableMetaData = testData.tableMetadata().TableWith2PartitionKeys3ClusteringKeys();
    return newFixture(
        tableMetaData, testData.logicalExpression().eqOnColumnThatIsNotOnSAI(tableMetaData));
  }

  public WhereAnalyzerFixture emptyFilter() {
    var tableMetaData = testData.tableMetadata().TableWith2PartitionKeys3ClusteringKeys();
    return newFixture(tableMetaData, testData.logicalExpression().empty());
  }

  private WhereAnalyzerFixture newFixture(
      TableMetadata tableMetadata, DBLogicalExpression logicalExpression) {
    return new WhereAnalyzerFixture(tableMetadata, logicalExpression);
  }

  public class WhereAnalyzerFixture {

    private final WhereCQLClauseAnalyzer analyzer;
    private final TableMetadata tableMetadata;
    private final DBLogicalExpression dBLogicalExpression;
    private WhereCQLClauseAnalyzer.WhereClauseAnalysis analysisResult = null;

    public WhereAnalyzerFixture(
        TableMetadata tableMetadata, DBLogicalExpression dBLogicalExpression) {
      this.analyzer = new WhereCQLClauseAnalyzer(new TableSchemaObject(tableMetadata));
      this.tableMetadata = tableMetadata;
      this.dBLogicalExpression = dBLogicalExpression;
    }

    public WhereAnalyzerFixture analyze() {
      // store the result in this fixture for later
      analysisResult = analyzer.analyse(dBLogicalExpression);
      return this;
    }

    public WhereAnalyzerFixture assertAnalysisResultNotNull() {
      assertThat(analysisResult).isNotNull();
      return this;
    }

    public WhereAnalyzerFixture assertAllowFiltering() {
      assertThat(analysisResult.requiresAllowFiltering()).isTrue();
      return this;
    }

    public WhereAnalyzerFixture assertNotAllowFiltering() {
      assertThat(analysisResult.requiresAllowFiltering()).isFalse();
      return this;
    }

    public WhereAnalyzerFixture assertWarningExceptions(WarningException.Code warningCode) {
      // Filter the warning exceptions based on the provided warningCode
      List<WarningException> filteredWarnings =
          analysisResult.warningExceptions().stream()
              .filter(exception -> exception.code.equals(warningCode.name()))
              .collect(Collectors.toList());

      // Assert that the filtered list of warnings is not empty (or perform other assertions)
      assertThat(filteredWarnings).isNotEmpty();
      return this;
    }

    public WhereAnalyzerFixture assertNoWarningExceptions() {
      assertThat(analysisResult.warningExceptions()).isEmpty();
      return this;
    }

    public void doThrowOnExecute(Throwable expectedException) {
      doThrow(expectedException).when(analyze());
    }
  }
}
