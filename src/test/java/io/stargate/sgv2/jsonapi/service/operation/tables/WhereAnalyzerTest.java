package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.LogicalExpressionTestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import org.junit.jupiter.api.Test;

/** Tests for the {@link WhereCQLClauseAnalyzer}. */
public class WhereAnalyzerTest {

  private static final TestData TEST_DATA = new TestData();

  private TestDataNames names() {
    return TEST_DATA.names;
  }

  // ==================================================================================================================
  // EASY CASES
  // (there are a number of combinations to test, pls keep organised into sections for easier
  // reading)
  // ==================================================================================================================

  @Test
  public void emptyFilter() {
    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("emptyFilter()");
    fixture
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.ZERO_FILTER_OPERATIONS);
  }

  @Test
  public void eqAllPrimaryKeys() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("eqAllPrimaryKeys()");
    fixture.expression().eqAllPrimaryKeys().analyze().assertNoFilteringNoWarnings();
  }

  // ==================================================================================================================
  // NON PK COLUMNS - INDEXED AND UNINDEXED
  // ==================================================================================================================

  @Test
  public void oneIndexed() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("oneIndexed()");
    fixture.expression().eqOn(names().COL_INDEXED_1).analyze().assertNoFilteringNoWarnings();
  }

  @Test
  public void oneIndexedOneRegular() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("oneIndexedOneRegular()");

    fixture
        .expression()
        .eqOn(names().COL_INDEXED_1)
        .expression()
        .eqOn(names().COL_REGULAR_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.MISSING_INDEX)
        .assertWarnOnUnindexedColumns(names().COL_REGULAR_1);
  }

  @Test
  public void oneRegular() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("oneRegular()");
    fixture
        .expression()
        .eqOn(names().COL_REGULAR_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.MISSING_INDEX)
        .assertWarnOnUnindexedColumns(names().COL_REGULAR_1);
  }

  // ==================================================================================================================
  // NOT EQUAL OPERATIONS - INDEXED AND UNINDEXED
  // ==================================================================================================================

  @Test
  public void notEqTextOnIndexed() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("notEqTextOnIndexed()");
    fixture
        .expression()
        .notEqOn(names().COL_INDEXED_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING)
        .assertWarnOnNotEqColumns(names().COL_INDEXED_1);
  }

  @Test
  public void notEqTextOnRegular() {
    // this should be a regular missing index warning
    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("notEqTextOnRegular()");
    fixture
        .expression()
        .notEqOn(names().COL_REGULAR_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.MISSING_INDEX)
        .assertWarnOnUnindexedColumns(names().COL_REGULAR_1);
  }

  // ==================================================================================================================
  // PARTITION KEY - MIXED WITH INDEXED AND UNINDEXED
  // ==================================================================================================================

  @Test
  public void allPartitionKeys() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("eqAllPartitionKeys()");
    fixture.expression().eqAllPartitionKeys().analyze().assertNoFilteringNoWarnings();
  }

  @Test
  public void onePartition() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("onePartition()");
    fixture
        .expression()
        .eqOn(names().COL_PARTITION_KEY_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.INCOMPLETE_PRIMARY_KEY_FILTER)
        .assertWarnOnMissingPartitionKeys(names().COL_PARTITION_KEY_2);
  }

  @Test
  public void onePartitionOneRegular() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("onePartitionOneRegular()");
    fixture
        .expression()
        .eqOn(names().COL_PARTITION_KEY_1)
        .expression()
        .eqOn(names().COL_REGULAR_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.MISSING_INDEX)
        .assertWarnOnUnindexedColumns(names().COL_PARTITION_KEY_1, names().COL_REGULAR_1);
  }

  @Test
  public void onePartitionOneIndexed() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("onePartitionOneIndexed()");
    fixture
        .expression()
        .eqOn(names().COL_PARTITION_KEY_1)
        .expression()
        .eqOn(names().COL_INDEXED_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.MISSING_INDEX)
        .assertWarnOnUnindexedColumns(names().COL_PARTITION_KEY_1);
  }

  // ==================================================================================================================
  // CLUSTERING COLUMNS - FULL PARTITION KEY, PARTIAL CLUSTERING KEY
  // ==================================================================================================================

  @Test
  public void skip1of3ClusteringKey() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("skip1of3ClusteringKey()");
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqSkipOneClusteringKeys(0)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.INCOMPLETE_PRIMARY_KEY_FILTER)
        .assertWarnOnOutOfOrder(names().COL_CLUSTERING_KEY_2, names().COL_CLUSTERING_KEY_3);
  }

  @Test
  public void skip2of3ClusteringKey() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("skip2of3ClusteringKey()");
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqSkipOneClusteringKeys(1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.INCOMPLETE_PRIMARY_KEY_FILTER)
        .assertWarnOnOutOfOrder(names().COL_CLUSTERING_KEY_3);
  }

  @Test
  public void skip3of3ClusteringKey() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("skip3of3ClusteringKey()");
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqSkipOneClusteringKeys(2)
        .analyze()
        .assertNoFilteringNoWarnings();
  }

  @Test
  public void skip1and2of3ClusteringKey() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("skip3of3ClusteringKey()");
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqOnlyOneClusteringKey(2)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.INCOMPLETE_PRIMARY_KEY_FILTER)
        .assertWarnOnOutOfOrder(names().COL_CLUSTERING_KEY_3);
  }

  @Test
  public void skip2and3of3ClusteringKey() {

    var fixture =
        TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("skip2and3of3ClusteringKey()");
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqOnlyOneClusteringKey(0)
        .analyze()
        .assertNoFilteringNoWarnings();
  }

  @Test
  public void skip1and3of3ClusteringKey() {

    var fixture =
        TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("skip2and3of3ClusteringKey()");
    fixture
        .expression()
        .eqAllPartitionKeys()
        .expression()
        .eqOnlyOneClusteringKey(1)
        .analyze()
        .assertOneWarning(WarningException.Code.INCOMPLETE_PRIMARY_KEY_FILTER)
        .assertWarnOnOutOfOrder(names().COL_CLUSTERING_KEY_2);
  }

  // ==================================================================================================================
  // ERRORS
  // ==================================================================================================================

  @Test
  public void oneUnknownColumn() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("oneUnknownColumn()");
    fixture
        .expression()
        .expression
        .addFilter(
            LogicalExpressionTestData.ExpressionBuilder.filter(
                names().COL_UNKNOWN_1, DataTypes.TEXT, NativeTypeTableFilter.Operator.EQ, "value"));

    fixture
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.UNKNOWN_TABLE_COLUMNS)
        .assertExceptionOnUnknownColumns(names().COL_UNKNOWN_1);
  }

  @Test
  public void unknownAndFullPk() {

    var fixture = TEST_DATA.whereAnalyzer().table2PK3Clustering1Index("unknownAndFullPk()");
    fixture
        .expression()
        .expression
        .addFilter(
            LogicalExpressionTestData.ExpressionBuilder.filter(
                names().COL_UNKNOWN_1, DataTypes.TEXT, NativeTypeTableFilter.Operator.EQ, "value"));

    fixture
        .expression()
        .eqAllPrimaryKeys()
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.UNKNOWN_TABLE_COLUMNS)
        .assertExceptionOnUnknownColumns(names().COL_UNKNOWN_1);
  }

  @Test
  public void eqOneDuration() {

    var fixture = TEST_DATA.whereAnalyzer().tableKeyAndTwoDuration("eqOneDuration()");

    fixture
        .expression()
        .eqOn(names().COL_REGULAR_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.MISSING_INDEX)
        .assertWarnOnUnindexedColumns(names().COL_REGULAR_1);
  }

  @Test
  public void gtOneDuration() {

    var fixture = TEST_DATA.whereAnalyzer().tableKeyAndTwoDuration("gtOnDuration()");

    fixture
        .expression()
        .gtOn(names().COL_REGULAR_1)
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.COMPARISON_FILTER_AGAINST_DURATION)
        .assertExceptionOnDurationColumns(names().COL_REGULAR_1);
  }

  @Test
  public void gtTwoDuration() {

    var fixture = TEST_DATA.whereAnalyzer().tableKeyAndTwoDuration("gtOnDuration()");

    fixture
        .expression()
        .gtOn(names().COL_REGULAR_1)
        .expression()
        .gtOn(names().COL_REGULAR_2)
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.COMPARISON_FILTER_AGAINST_DURATION)
        .assertExceptionOnDurationColumns(names().COL_REGULAR_1, names().COL_REGULAR_2);
  }

  @Test
  public void gtOneDurationFullPk() {

    var fixture = TEST_DATA.whereAnalyzer().tableKeyAndTwoDuration("gtOnDuration()");

    fixture
        .expression()
        .gtOn(names().COL_REGULAR_1)
        .expression()
        .eqAllPrimaryKeys()
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.COMPARISON_FILTER_AGAINST_DURATION)
        .assertExceptionOnDurationColumns(names().COL_REGULAR_1);
  }
}
