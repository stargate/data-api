package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.exception.FilterException;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestData;
import io.stargate.sgv2.jsonapi.fixtures.testdata.TestDataNames;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Tests for the {@link WhereCQLClauseAnalyzer}. Focus on Delete Statement type */
public class DeleteWhereAnalyzerTest {

  private static final TestData TEST_DATA = new TestData();

  private TestDataNames names() {
    return TEST_DATA.names;
  }

  // Method to provide parameters (DELETE_ONE and DELETE_MANY)
  private static Stream<WhereCQLClauseAnalyzer.StatementType> deleteStatementTypes() {
    return Stream.of(
        WhereCQLClauseAnalyzer.StatementType.DELETE_ONE,
        WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
  }

  @ParameterizedTest
  @MethodSource("deleteStatementTypes")
  public void emptyFilter(WhereCQLClauseAnalyzer.StatementType deleteStatementType) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("deleteOne_emptyFilter()", deleteStatementType);
    fixture
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.NO_FILTER_UPDATE_DELETE);
  }

  @ParameterizedTest
  @MethodSource("deleteStatementTypes")
  public void eqAllPrimaryKeys(WhereCQLClauseAnalyzer.StatementType deleteStatementType) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("deleteOne_eqAllPrimaryKeys()", deleteStatementType);
    fixture.expression().eqAllPrimaryKeys().analyze().assertNoFilteringNoWarnings();
  }

  @ParameterizedTest
  @MethodSource("deleteStatementTypes")
  public void eqOnNonPrimaryKey(WhereCQLClauseAnalyzer.StatementType deleteStatementType) {
    var fixture =
        TEST_DATA
            .whereAnalyzer()
            .table2PK3Clustering1Index("deleteOne_eqOnNonPrimaryKey()", deleteStatementType);
    fixture
        .expression()
        .eqFirstNonPKOrIndexed()
        .analyzeThrows(FilterException.class)
        .assertFilterExceptionCode(FilterException.Code.NON_PRIMARY_KEY_COLUMNS_USED_UPDATE_DELETE);
  }

  //  @ParameterizedTest
  //  @MethodSource("deleteStatementTypes")
  //  public void eqMissingPartitionKey(WhereCQLClauseAnalyzer.StatementType deleteStatementType) {
  //    var fixture =
  //            TEST_DATA
  //                    .whereAnalyzer()
  //                    .table2PK3Clustering1Index(
  //                            "deleteOne_missingPartitionKeys()", deleteStatementType);
  //    fixture
  //            .expression()
  //            .eqSkipOnePartitionKeys(0)
  //            .analyzeThrows(FilterException.class)
  //            .assertFilterExceptionCode(FilterException.Code.PRIMARY_KEY_NOT_FULLY_SPECIFIED);
  //  }

  // ==================================================================================================================
  // Special cases for DeleteOne.
  // Note, Full primary keys must be specified in API filter
  // ==================================================================================================================

  @Nested
  class DeleteOne {

    @Test
    public void eqMissingPartitionKey() {
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteOne_missingPartitionKeys()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_ONE);
      fixture
          .expression()
          .eqSkipOnePartitionKeys(0)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.PRIMARY_KEY_NOT_FULLY_SPECIFIED);
    }

    @Test
    public void skip1of3ClusteringKey() {
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteOne_skip1of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_ONE);
      fixture
          .expression()
          .eqAllPartitionKeys()
          .expression()
          .eqSkipOneClusteringKeys(1)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.PRIMARY_KEY_NOT_FULLY_SPECIFIED);
    }

    @Test
    public void skip2of3ClusteringKey() {
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteOne_skip2of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_ONE);
      fixture
          .expression()
          .eqAllPartitionKeys()
          .expression()
          .eqSkipOneClusteringKeys(1)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.PRIMARY_KEY_NOT_FULLY_SPECIFIED);
    }

    @Test
    public void skip3of3ClusteringKey() {
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteOne_skip3of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_ONE);
      fixture
          .expression()
          .eqAllPartitionKeys()
          .expression()
          .eqSkipOneClusteringKeys(2)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.PRIMARY_KEY_NOT_FULLY_SPECIFIED);
    }
  }

  // ==================================================================================================================
  // Special cases for DeleteMany.
  // Note, API filter can do partial primary keys, but still needs to be a VALID PK filtering.
  // VALID means all partition keys specified and not skipping clustering keys.
  // ==================================================================================================================

  @Nested
  class DeleteMany {

    @Test
    public void eqMissingPartitionKey() {
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteMany_missingPartitionKeys()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
      fixture
          .expression()
          .eqSkipOnePartitionKeys(0)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER);
    }

    @Test
    public void skip1of3ClusteringKey() {
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteMany_skip1of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
      fixture
          .expression()
          .eqAllPartitionKeys()
          .expression()
          .eqSkipOneClusteringKeys(1)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER);
    }

    @Test
    public void skip2of3ClusteringKey() {
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteMany_skip2of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
      fixture
          .expression()
          .eqAllPartitionKeys()
          .expression()
          .eqSkipOneClusteringKeys(1)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER);
    }

    @Test
    public void skip3of3ClusteringKey() {
      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteMany_skip3of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
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
                  "deleteMany_skip3of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
      fixture
          .expression()
          .eqAllPartitionKeys()
          .expression()
          .eqOnlyOneClusteringKey(2)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER);
    }

    @Test
    public void skip2and3of3ClusteringKey() {

      var fixture =
          TEST_DATA
              .whereAnalyzer()
              .table2PK3Clustering1Index(
                  "deleteMany_skip2and3of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
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
                  "deleteMany_skip1and3of3ClusteringKey()",
                  WhereCQLClauseAnalyzer.StatementType.DELETE_MANY);
      fixture
          .expression()
          .eqAllPartitionKeys()
          .expression()
          .eqOnlyOneClusteringKey(1)
          .analyzeThrows(FilterException.class)
          .assertFilterExceptionCode(FilterException.Code.INCOMPLETE_PRIMARY_KEY_FILTER);
    }
  }
}
