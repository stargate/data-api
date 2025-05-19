package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataTypes;
import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.exception.WarningException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.LogicalExpressionTestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.NativeTypeTableFilter;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for the {@link WhereCQLClauseAnalyzer}. Focus on Select Statement type */
public class SelectWhereAnalyzerTest {

  private static final TestData TEST_DATA = new TestData();

  private static TestDataNames names() {
    return TEST_DATA.names;
  }

  // ==================================================================================================================
  // EASY CASES
  // (there are a number of combinations to test, pls keep organised into sections for easier
  // reading)
  // ==================================================================================================================

  @Test
  public void emptyFilter() {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "emptyFilter()", WhereCQLClauseAnalyzer.StatementType.SELECT);
    fixture
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.ZERO_FILTER_OPERATIONS);
  }

  @Test
  public void eqAllPrimaryKeys() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqAllPrimaryKeys()", WhereCQLClauseAnalyzer.StatementType.SELECT);
    fixture.expression().eqAllPrimaryKeys().analyze().assertNoFilteringNoWarnings();
  }

  // ==================================================================================================================
  // NON PK COLUMNS - INDEXED AND UNINDEXED
  // ==================================================================================================================

  @Test
  public void oneIndexed() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("oneIndexed()", WhereCQLClauseAnalyzer.StatementType.SELECT);
    fixture.expression().eqOn(names().COL_INDEXED_1).analyze().assertNoFilteringNoWarnings();
  }

  @Test
  public void oneIndexedOneRegular() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "oneIndexedOneRegular()", WhereCQLClauseAnalyzer.StatementType.SELECT);

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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("oneRegular()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "notEqTextOnIndexed()", WhereCQLClauseAnalyzer.StatementType.SELECT);
    fixture
        .expression()
        .notEqOn(names().COL_INDEXED_1)
        .analyze()
        .assertAllowFilteringEnabled()
        .assertOneWarning(WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING)
        .assertWarnOnNotEqColumns(names().COL_INDEXED_1);
  }

  // TODO cases for supported $ne on indexed column dataType

  @Test
  public void notEqTextOnRegular() {
    // this should be a regular missing index warning
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "notEqTextOnRegular()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "eqAllPartitionKeys()", WhereCQLClauseAnalyzer.StatementType.SELECT);
    fixture.expression().eqAllPartitionKeys().analyze().assertNoFilteringNoWarnings();
  }

  @Test
  public void onePartition() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "onePartition()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "onePartitionOneRegular()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "onePartitionOneIndexed()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "skip1of3ClusteringKey()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "skip2of3ClusteringKey()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "skip3of3ClusteringKey()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "skip3of3ClusteringKey()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "skip2and3of3ClusteringKey()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "skip2and3of3ClusteringKey()", WhereCQLClauseAnalyzer.StatementType.SELECT);
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "oneUnknownColumn()", WhereCQLClauseAnalyzer.StatementType.SELECT);
    fixture
        .expression()
        .rootImplicitAnd
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index(
                "unknownAndFullPk()", WhereCQLClauseAnalyzer.StatementType.SELECT);
    fixture
        .expression()
        .rootImplicitAnd
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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .tableKeyAndTwoDuration("eqOneDuration()", WhereCQLClauseAnalyzer.StatementType.SELECT);

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

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .tableKeyAndTwoDuration("gtOnDuration()", WhereCQLClauseAnalyzer.StatementType.SELECT);

    fixture
        .expression()
        .gtOn(names().COL_REGULAR_1)
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(
            FilterException.Code.UNSUPPORTED_COMPARISON_FILTER_AGAINST_DURATION)
        .assertExceptionOnDurationColumns(names().COL_REGULAR_1);
  }

  @Test
  public void gtTwoDuration() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .tableKeyAndTwoDuration("gtOnDuration()", WhereCQLClauseAnalyzer.StatementType.SELECT);

    fixture
        .expression()
        .gtOn(names().COL_REGULAR_1)
        .expression()
        .gtOn(names().COL_REGULAR_2)
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(
            FilterException.Code.UNSUPPORTED_COMPARISON_FILTER_AGAINST_DURATION)
        .assertExceptionOnDurationColumns(names().COL_REGULAR_1, names().COL_REGULAR_2);
  }

  @Test
  public void gtOneDurationFullPk() {

    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .tableKeyAndTwoDuration("gtOnDuration()", WhereCQLClauseAnalyzer.StatementType.SELECT);

    fixture
        .expression()
        .gtOn(names().COL_REGULAR_1)
        .expression()
        .eqAllPrimaryKeys()
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(
            FilterException.Code.UNSUPPORTED_COMPARISON_FILTER_AGAINST_DURATION)
        .assertExceptionOnDurationColumns(names().COL_REGULAR_1);
  }

  // ==================================================================================================================
  // Sanity Check for all column datatypes we support
  // ==================================================================================================================

  @Nested
  class DataTypesSanityCheck {

    private static Stream<Arguments> allScalarDatatype() {
      return names().ALL_SCALAR_DATATYPE_COLUMNS.stream().map(Arguments::of);
    }

    private static final Set<CqlIdentifier> allow_filtering_needed_for_comparison =
        Set.of(
            names().CQL_TEXT_COLUMN,
            names().CQL_ASCII_COLUMN,
            names().CQL_BOOLEAN_COLUMN,
            names().CQL_UUID_COLUMN);

    private static final Set<CqlIdentifier> allow_filtering_needed_for_not_in =
        Set.of(
            names().CQL_TEXT_COLUMN,
            names().CQL_ASCII_COLUMN,
            names().CQL_BOOLEAN_COLUMN,
            names().CQL_UUID_COLUMN);

    // ==================================================================================================================
    // $eq on scalar column
    // -> column is indexed.
    // -> column is not indexed.
    // ==================================================================================================================

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void eq_indexed_column(CqlIdentifier cqlDatatypeColumn) {

      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)
          || cqlDatatypeColumn.equals(names().CQL_DURATION_COLUMN)) {
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesIndexed(
                  "$eq_on_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture.expression().eqOn(cqlDatatypeColumn).analyze().assertNoFilteringNoWarnings();

      // Duration Column can not be indexed
      var durationFixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesIndexed(
                  "$eq_on_" + names().CQL_DURATION_COLUMN.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      durationFixture
          .expression()
          .eqOn(names().CQL_DURATION_COLUMN)
          .analyze()
          .assertAllowFilteringEnabled()
          .assertOneWarning(WarningException.Code.MISSING_INDEX)
          .assertWarnOnUnindexedColumns(names().CQL_DURATION_COLUMN);
    }

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void eq_not_indexed_column(CqlIdentifier cqlDatatypeColumn) {
      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesNotIndexed(
                  "$eq_on_not_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture
          .expression()
          .eqOn(cqlDatatypeColumn)
          .analyze()
          .assertAllowFilteringEnabled()
          .assertOneWarning(WarningException.Code.MISSING_INDEX)
          .assertWarnOnUnindexedColumns(cqlDatatypeColumn);
    }

    // ==================================================================================================================
    // $ne on scalar column
    // -> column is indexed.
    // -> column is not indexed.
    // ==================================================================================================================

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void ne_indexed_column(CqlIdentifier cqlDatatypeColumn) {
      // If column is on SAI index, perform $ne on it.
      // -> For TEXT, ASCII, BOOLEAN, DURATION, BLOB. ALLOW FILTERING is needed.
      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      if (cqlDatatypeColumn.equals(names().CQL_DURATION_COLUMN)) {
        var fixture =
            TEST_DATA
                .whereAnalyzer()
                .tableAllColumnDatatypesIndexed(
                    "$ne_on_" + cqlDatatypeColumn.asInternal(),
                    WhereCQLClauseAnalyzer.StatementType.SELECT);
        fixture
            .expression()
            .notEqOn(cqlDatatypeColumn)
            .analyze()
            .assertAllowFilteringEnabled()
            .assertOneWarning(WarningException.Code.MISSING_INDEX)
            .assertWarnOnUnindexedColumns(cqlDatatypeColumn);
        return;
      }
      if (cqlDatatypeColumn.equals(names().CQL_TEXT_COLUMN)
          || cqlDatatypeColumn.equals(names().CQL_BOOLEAN_COLUMN)
          || cqlDatatypeColumn.equals(names().CQL_ASCII_COLUMN)
          || cqlDatatypeColumn.equals(names().CQL_UUID_COLUMN)) {
        var fixture =
            TEST_DATA
                .whereAnalyzer()
                .tableAllColumnDatatypesIndexed(
                    "$ne_on_indexed_" + cqlDatatypeColumn.asInternal(),
                    WhereCQLClauseAnalyzer.StatementType.SELECT);
        fixture
            .expression()
            .notEqOn(cqlDatatypeColumn)
            .analyze()
            .assertAllowFilteringEnabled()
            .assertOneWarning(WarningException.Code.NOT_EQUALS_UNSUPPORTED_BY_INDEXING)
            .assertWarnOnNotEqColumns(cqlDatatypeColumn);
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesIndexed(
                  "$ne_on_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture.expression().notEqOn(cqlDatatypeColumn).analyze().assertNoFilteringNoWarnings();
    }

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void ne_not_indexed_column(CqlIdentifier cqlDatatypeColumn) {

      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesNotIndexed(
                  "$ne_on_not_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture
          .expression()
          .notEqOn(cqlDatatypeColumn)
          .analyze()
          .assertAllowFilteringEnabled()
          .assertOneWarning(WarningException.Code.MISSING_INDEX)
          .assertWarnOnUnindexedColumns(cqlDatatypeColumn);
    }

    // ==================================================================================================================
    // $in on scalar column
    // -> column is indexed.
    // -> column is not indexed.
    // ==================================================================================================================

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void in_indexed_column(CqlIdentifier cqlDatatypeColumn) {

      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      if (cqlDatatypeColumn.equals(names().CQL_DURATION_COLUMN)) {
        var fixture =
            TEST_DATA
                .whereAnalyzer()
                .tableAllColumnDatatypesIndexed(
                    "$in_on_" + cqlDatatypeColumn.asInternal(),
                    WhereCQLClauseAnalyzer.StatementType.SELECT);
        fixture
            .expression()
            .inOn(cqlDatatypeColumn)
            .analyze()
            .assertAllowFilteringEnabled()
            .assertOneWarning(WarningException.Code.MISSING_INDEX)
            .assertWarnOnUnindexedColumns(cqlDatatypeColumn);
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesIndexed(
                  "$in_on_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture.expression().inOn(cqlDatatypeColumn).analyze().assertNoFilteringNoWarnings();
    }

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void in_not_indexed_column(CqlIdentifier cqlDatatypeColumn) {

      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesNotIndexed(
                  "$in_on_not_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture
          .expression()
          .inOn(cqlDatatypeColumn)
          .analyze()
          .assertAllowFilteringEnabled()
          .assertOneWarning(WarningException.Code.MISSING_INDEX)
          .assertWarnOnUnindexedColumns(cqlDatatypeColumn);
    }

    // ==================================================================================================================
    // $nin on scalar column
    // -> column is indexed.
    // -> column is not indexed.
    // ==================================================================================================================

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void nin_indexed_column(CqlIdentifier cqlDatatypeColumn) {

      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      if (cqlDatatypeColumn.equals(names().CQL_DURATION_COLUMN)) {
        var fixture =
            TEST_DATA
                .whereAnalyzer()
                .tableAllColumnDatatypesIndexed(
                    "$nin_on_" + cqlDatatypeColumn.asInternal(),
                    WhereCQLClauseAnalyzer.StatementType.SELECT);
        fixture
            .expression()
            .notInOn(cqlDatatypeColumn)
            .analyze()
            .assertAllowFilteringEnabled()
            .assertOneWarning(WarningException.Code.MISSING_INDEX)
            .assertWarnOnUnindexedColumns(cqlDatatypeColumn);
        return;
      }
      if (allow_filtering_needed_for_not_in.contains(cqlDatatypeColumn)) {
        var fixture =
            TEST_DATA
                .whereAnalyzer()
                .tableAllColumnDatatypesIndexed(
                    "$nin_on_" + cqlDatatypeColumn.asInternal(),
                    WhereCQLClauseAnalyzer.StatementType.SELECT);
        fixture
            .expression()
            .notInOn(cqlDatatypeColumn)
            .analyze()
            .assertAllowFilteringEnabled()
            .assertOneWarning(WarningException.Code.NOT_IN_FILTER_UNSUPPORTED_BY_INDEXING)
            .assertWarnOnNotInColumns(cqlDatatypeColumn);
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesIndexed(
                  "$nin_on_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture.expression().notInOn(cqlDatatypeColumn).analyze().assertNoFilteringNoWarnings();
    }

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void not_in_not_indexed_column(CqlIdentifier cqlDatatypeColumn) {

      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesNotIndexed(
                  "$nin_on_not_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture
          .expression()
          .notInOn(cqlDatatypeColumn)
          .analyze()
          .assertAllowFilteringEnabled()
          .assertOneWarning(WarningException.Code.MISSING_INDEX)
          .assertWarnOnUnindexedColumns(cqlDatatypeColumn);
    }

    // ==================================================================================================================
    // Api comparison operator on scalar column, take $gt as example.
    // 1.Can not apply $gt/$lte/$gte/$lte to Duration column
    // 2.Can not apply to indexed UUID column, index does not support operator
    // ==================================================================================================================

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void comparison_api_filter_indexed_column(CqlIdentifier cqlDatatypeColumn) {

      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      if (cqlDatatypeColumn.equals(names().CQL_DURATION_COLUMN)) {
        var fixture =
            TEST_DATA
                .whereAnalyzer()
                .tableAllColumnDatatypesIndexed(
                    "$gt_on_" + cqlDatatypeColumn.asInternal(),
                    WhereCQLClauseAnalyzer.StatementType.SELECT);
        fixture
            .expression()
            .gtOn(cqlDatatypeColumn)
            .analyzeThrows(FilterException.class)
            .assertFilterExceptionCode(
                FilterException.Code.UNSUPPORTED_COMPARISON_FILTER_AGAINST_DURATION)
            .assertExceptionOnDurationColumns(cqlDatatypeColumn);
        return;
      }

      if (names().COMPARISON_WITH_INDEX_WARN_COLUMNS.contains(cqlDatatypeColumn)) {
        var fixture =
            TEST_DATA
                .whereAnalyzer()
                .tableAllColumnDatatypesIndexed(
                    "$gt_on_" + cqlDatatypeColumn.asInternal(),
                    WhereCQLClauseAnalyzer.StatementType.SELECT);
        fixture
            .expression()
            .gtOn(cqlDatatypeColumn)
            .analyze()
            .assertAllowFilteringEnabled()
            .assertOneWarning(WarningException.Code.COMPARISON_FILTER_UNSUPPORTED_BY_INDEXING)
            .assertWarnOnComparisonFilterColumns(cqlDatatypeColumn);
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesIndexed(
                  "$gt_on_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture.expression().gtOn(cqlDatatypeColumn).analyze().assertNoFilteringNoWarnings();
    }

    @ParameterizedTest
    @MethodSource("allScalarDatatype")
    public void comparison_api_filter_not_indexed_column(CqlIdentifier cqlDatatypeColumn) {

      if (cqlDatatypeColumn.equals(names().CQL_BLOB_COLUMN)) {
        return;
      }
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .tableAllColumnDatatypesNotIndexed(
                  "$gt_on_not_indexed_" + cqlDatatypeColumn.asInternal(),
                  WhereCQLClauseAnalyzer.StatementType.SELECT);
      fixture
          .expression()
          .inOn(cqlDatatypeColumn)
          .analyze()
          .assertAllowFilteringEnabled()
          .assertOneWarning(WarningException.Code.MISSING_INDEX)
          .assertWarnOnUnindexedColumns(cqlDatatypeColumn);
    }
  }
}
